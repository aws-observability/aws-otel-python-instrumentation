FROM public.ecr.aws/sam/build-python3.12 AS python312

ADD . /workspace

WORKDIR /workspace

RUN sed -i "/opentelemetry-exporter-otlp-proto-grpc/d" ./aws-opentelemetry-distro/pyproject.toml

RUN sed -i "/opentelemetry-instrumentation-system-metrics/d" ./aws-opentelemetry-distro/pyproject.toml

RUN mkdir -p /build && \
  python3 -m pip install aws-opentelemetry-distro/ -t /build/python && \
  mv otel_wrapper.py /build/python && \
  mv otel-instrument /build && \
  python3 -m compileall /build/python && \
  chmod 755 /build/otel-instrument && \
  rm -rf /build/python/urllib3*


FROM public.ecr.aws/sam/build-python3.11 AS python311

WORKDIR /workspace

COPY --from=python312 /build /build

RUN python3 -m compileall /build/python

RUN cd /build && \
  zip -r aws-opentelemetry-python-layer.zip otel-instrument python

CMD ["cp", "/build/aws-opentelemetry-python-layer.zip", "/out/aws-opentelemetry-python-layer.zip"]
