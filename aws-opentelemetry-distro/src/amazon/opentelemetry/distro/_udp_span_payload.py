# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Size-aware payload builder for exporting spans over UDP.

A UDP datagram cannot exceed 65,507 bytes. Spans are split into as many datagrams as needed, and a
single span that is still too large has its longest string attribute values truncated so it fits.
Truncated values and spans are marked so users can tell what was cut.

This module is imported by the lite SDK, so it must stay free of heavy imports at module load time.
"""
from logging import Logger, getLogger
from typing import Any, Callable, Dict, Iterator, List, MutableMapping, Optional, Sequence, Tuple, TypeVar

_logger: Logger = getLogger(__name__)

# Maximum UDP payload over IPv4: 65,535 - 8 byte UDP header - 20 byte IP header
MAX_UDP_DATAGRAM_SIZE = 65_507

TRUNCATED_ATTRIBUTE_KEY = "aws.otel.udp.truncated"
# Each truncated value keeps at least this many bytes so it stays readable
_MIN_KEPT_VALUE_SIZE = 256
_MAX_TRUNCATION_ROUNDS = 3

SpanT = TypeVar("SpanT")


def max_payload_size(message_overhead: int) -> int:
    """Largest protobuf payload whose base64 encoding plus overhead fits in one datagram."""
    return (MAX_UDP_DATAGRAM_SIZE - message_overhead) // 4 * 3


def build_payloads(
    spans: Sequence[SpanT],
    encode: Callable[[Sequence[SpanT]], bytes],
    max_size: int,
    truncate: Optional[Callable[[SpanT], Optional[bytes]]] = None,
) -> Iterator[Tuple[int, Optional[bytes]]]:
    """Yield (span_count, payload) pairs, each payload fitting within max_size.

    The common case encodes the batch once and yields it unchanged. Oversized batches are split into chunks
    sized from the encoded length, so each span is re-encoded about once rather than once per halving level;
    a chunk that is still too large is split again. A single oversized span is truncated, by default by
    truncate_span_payload on its encoded OTLP request. payload is None if a span cannot be made to fit.
    """
    data = encode(spans)
    if len(data) <= max_size:
        yield len(spans), data
        return
    if len(spans) == 1:
        yield 1, truncate(spans[0]) if truncate else truncate_span_payload(data, max_size)
        return
    # Target 90% of the limit so chunks of unevenly sized spans usually fit on the first try
    chunk_count = min(len(spans), max(2, -(-len(data) * 10 // (max_size * 9))))
    chunk_size = -(-len(spans) // chunk_count)
    for start in range(0, len(spans), chunk_size):
        yield from build_payloads(spans[start : start + chunk_size], encode, max_size, truncate)


def truncate_span_payload(data: bytes, max_size: int) -> Optional[bytes]:
    """Shrink an encoded ExportTraceServiceRequest to max_size, see truncate_to_fit.

    Only span, event and link attributes are truncated; resource and scope attributes are left as is.
    """
    # pylint: disable=import-outside-toplevel,no-name-in-module
    from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest

    request = ExportTraceServiceRequest()
    request.ParseFromString(data)
    spans = [span for rs in request.resource_spans for ss in rs.scope_spans for span in ss.spans]
    return truncate_to_fit(
        lambda: _string_values(spans),
        request.SerializeToString,
        lambda paths_by_span: _set_truncated_markers(spans, paths_by_span),
        max_size,
        len(spans),
    )


def truncate_to_fit(
    collect: Callable[[], List[Tuple[int, str, Any]]],
    serialize: Callable[[], bytes],
    set_markers: Callable[[Dict[int, List[str]]], None],
    max_size: int,
    span_count: int,
) -> Optional[bytes]:
    """Cut the longest string values down to a common size until serialize() fits in max_size.

    collect returns (span index, attribute path, slot) for every truncatable value, where slot exposes a
    read/write string_value. Each truncated value ends with a suffix giving its original and kept size, and
    set_markers is given the truncated paths of each span so it can record them under TRUNCATED_ATTRIBUTE_KEY.
    Returns the serialized payload, or None if it still does not fit.
    """
    # (span index, attribute path) -> original value, in truncation order
    originals: Dict[Tuple[int, str], str] = {}

    for _ in range(_MAX_TRUNCATION_ROUNDS):
        serialized = serialize()
        need = len(serialized) - max_size
        if need <= 0:
            _logger.warning(
                "Truncated %s to fit the %d byte UDP limit",
                ", ".join(path for _, path in originals),
                MAX_UDP_DATAGRAM_SIZE,
            )
            return serialized
        if not originals:
            # The first truncation adds the marker attribute key
            need += len(TRUNCATED_ATTRIBUTE_KEY) + 8
        candidates = collect()
        keep = _uniform_keep_size([_truncation_cost(*candidate, originals) for candidate in candidates], need)
        for span_index, path, value in candidates:
            _truncate_value(span_index, path, value, keep, originals)
        paths_by_span: Dict[int, List[str]] = {}
        for span_index, path in originals:
            paths_by_span.setdefault(span_index, []).append(path)
        set_markers(paths_by_span)

    serialized = serialize()
    if len(serialized) <= max_size:
        return serialized
    _logger.warning("Dropping %d span(s): %d bytes after truncation exceeds the UDP limit", span_count, len(serialized))
    return None


def _truncation_cost(span_index: int, path: str, value, originals: Dict[Tuple[int, str], str]) -> Tuple[int, int]:
    """Return (current size, fixed overhead of truncating) for a value."""
    original = originals.get((span_index, path), value.string_value)
    # Kept size is unknown until the value is cut, so reserve room for the longest possible suffix
    overhead = len(_truncation_suffix(len(original.encode("utf-8")), len(original.encode("utf-8"))))
    if (span_index, path) not in originals:
        # A newly truncated path also grows the marker attribute listing it
        overhead += len(path.encode("utf-8")) + 8
    return len(value.string_value.encode("utf-8")), overhead


def _uniform_keep_size(costs: List[Tuple[int, int]], need: int) -> int:
    """Largest size that saves at least need bytes when every longer value is cut down to it.

    Cutting all oversized values to one common size keeps as much of each value as possible, instead of
    emptying the longest value while leaving the next longest untouched.
    """

    def saved(keep: int) -> int:
        return sum(size - keep - overhead for size, overhead in costs if size > keep + overhead)

    low = _MIN_KEPT_VALUE_SIZE
    high = max((size for size, _ in costs), default=low)
    if saved(low) < need:
        return low
    while low < high:
        mid = (low + high + 1) // 2
        if saved(mid) >= need:
            low = mid
        else:
            high = mid - 1
    return low


def _truncate_value(span_index: int, path: str, value, keep: int, originals: Dict[Tuple[int, str], str]) -> None:
    """Cut a value down to keep bytes plus the truncation suffix, if it is longer than that."""
    original = originals.get((span_index, path), value.string_value)
    original_size = len(original.encode("utf-8"))
    if len(value.string_value.encode("utf-8")) <= keep + len(_truncation_suffix(original_size, original_size)):
        return
    kept = _utf8_truncate(original, keep)
    value.string_value = kept + _truncation_suffix(original_size, len(kept.encode("utf-8")))
    originals[(span_index, path)] = original


def _truncation_suffix(original_size: int, kept_size: int) -> str:
    return f"...[truncated: {original_size} -> {kept_size} bytes]"


def _set_truncated_markers(spans, paths_by_span: Dict[int, List[str]]) -> None:
    # pylint: disable=import-outside-toplevel,no-name-in-module
    from opentelemetry.proto.common.v1.common_pb2 import AnyValue, ArrayValue, KeyValue

    for span_index, paths in paths_by_span.items():
        attributes = spans[span_index].attributes
        for attribute_index, key_value in enumerate(attributes):
            if key_value.key == TRUNCATED_ATTRIBUTE_KEY:
                del attributes[attribute_index]
                break
        marker = AnyValue(array_value=ArrayValue(values=[AnyValue(string_value=path) for path in paths]))
        attributes.append(KeyValue(key=TRUNCATED_ATTRIBUTE_KEY, value=marker))


def _string_values(spans) -> List[Tuple[int, str, object]]:
    """Return (span index, attribute path, AnyValue) for every string value in span, event and link attributes."""
    values: List[Tuple[int, str, object]] = []
    for span_index, span in enumerate(spans):
        _collect_string_values(span_index, "", span.attributes, values)
        for event_index, event in enumerate(span.events):
            _collect_string_values(span_index, f"events[{event_index}].", event.attributes, values)
        for link_index, link in enumerate(span.links):
            _collect_string_values(span_index, f"links[{link_index}].", link.attributes, values)
    return values


def _collect_string_values(span_index: int, prefix: str, key_values, values: List) -> None:
    for key_value in key_values:
        if key_value.key != TRUNCATED_ATTRIBUTE_KEY:
            _collect_any_value(span_index, prefix + key_value.key, key_value.value, values)


def _collect_any_value(span_index: int, path: str, value, values: List) -> None:
    kind = value.WhichOneof("value")
    if kind == "string_value":
        values.append((span_index, path, value))
    elif kind == "array_value":
        for item_index, item in enumerate(value.array_value.values):
            _collect_any_value(span_index, f"{path}[{item_index}]", item, values)
    elif kind == "kvlist_value":
        _collect_string_values(span_index, path + ".", value.kvlist_value.values, values)


class _MappingSlot:
    """A string attribute value stored in a mutable mapping, optionally as an item of a sequence value."""

    def __init__(self, attributes: MutableMapping[str, Any], key: str, index: Optional[int] = None):
        self._attributes = attributes
        self._key = key
        self._index = index

    @property
    def string_value(self) -> str:
        value = self._attributes[self._key]
        return value if self._index is None else value[self._index]

    @string_value.setter
    def string_value(self, new_value: str) -> None:
        if self._index is None:
            self._attributes[self._key] = new_value
        else:
            items = list(self._attributes[self._key])
            items[self._index] = new_value
            self._attributes[self._key] = tuple(items)


def mapping_string_values(
    span_index: int, prefix: str, attributes: MutableMapping[str, Any]
) -> List[Tuple[int, str, Any]]:
    """Return truncate_to_fit slots for the string values (and string sequence items) of a plain attribute dict."""
    values: List[Tuple[int, str, Any]] = []
    for key, value in attributes.items():
        if key == TRUNCATED_ATTRIBUTE_KEY:
            continue
        if isinstance(value, str):
            values.append((span_index, prefix + key, _MappingSlot(attributes, key)))
        elif isinstance(value, (list, tuple)):
            for item_index, item in enumerate(value):
                if isinstance(item, str):
                    values.append(
                        (span_index, f"{prefix}{key}[{item_index}]", _MappingSlot(attributes, key, item_index))
                    )
    return values


def _utf8_truncate(value: str, max_bytes: int) -> str:
    # errors="ignore" drops a multi-byte character cut in half at the end
    return value.encode("utf-8")[:max_bytes].decode("utf-8", errors="ignore")
