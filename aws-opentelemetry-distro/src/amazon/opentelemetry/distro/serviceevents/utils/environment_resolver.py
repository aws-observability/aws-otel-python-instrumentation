# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Resolve ``aws.local.environment`` from OTel Resource attributes.

Mirrors the CloudWatch agent's awsapplicationsignals resolver precedence so the SDK can compute
the same environment value the agent would, with no dependency on the agent process:

    1. Explicit deployment.environment[.name] -> use as-is
    2. EKS / K8s -> "eks:<cluster>/<namespace>" or "k8s:<cluster>/<namespace>"
    3. ECS -> "ecs:<cluster>" (cluster name from aws.ecs.cluster.arn)
    4. EC2 (cloud.platform == aws_ec2) -> "ec2:<asg>" when an ASG is known, else "ec2:default"
    5. Otherwise (non-AWS / undetected host) -> "generic:default"

The EC2 branch is gated on the platform actually being EC2, mirroring the CloudWatch agent,
whose environment branches only run for EC2 (Platform == ModeEC2) or Kubernetes. On a
non-AWS / non-K8s host the CloudWatch agent runs its "generic" resolver (Mode == onPremise)
and emits "generic:default" -- it never leaves Environment empty -- so the SDK matches by
returning "generic:default" rather than empty. (Verified live: blocking IMDS makes the agent
fall to the generic resolver / "generic:default".)

Scope is the LOCAL environment only (aws.local.environment); remote-environment
correlation is out of scope (it depends on the agent's cluster-wide pod watcher).
"""
from typing import Callable, Dict, Mapping, Optional

AWS_LOCAL_ENVIRONMENT_KEY = "aws.local.environment"

# Attributes that indicate the resource has real platform context (i.e. detection has populated),
# used to distinguish a genuine fallback from a still-populating resource.
_PLATFORM_CONTEXT_KEYS = ("cloud.platform", "k8s.cluster.name", "aws.ecs.cluster.arn", "host.id")


def resolve_local_environment(
    attributes: Mapping[str, object], asg_supplier: Optional[Callable[[], str]] = None
) -> str:
    """Resolve aws.local.environment from the given resource attributes.

    asg_supplier, when provided, supplies the EC2 Auto Scaling group name and is invoked
    ONLY if the resolver reaches the EC2 branch (so EKS/ECS/explicit-env never trigger an
    IMDS lookup). It is the fallback for callers whose resource does not already carry
    ``ec2.tag.aws:autoscaling:groupName`` (e.g. Dynamic Instrumentation reading the global
    resource, which intentionally omits the ASG tag).
    """

    def _str(key: str) -> str:
        value = attributes.get(key)
        if value is None:
            return ""
        text = str(value).strip()
        return text

    # 1. Explicit deployment.environment[.name] wins outright.
    explicit_env = _str("deployment.environment.name") or _str("deployment.environment")
    if explicit_env:
        return explicit_env

    # 2. Kubernetes (EKS / K8s): "<prefix>:<cluster>/<namespace>".
    k8s_cluster = _str("k8s.cluster.name")
    namespace = _str("k8s.namespace.name")
    cloud_platform = _str("cloud.platform")
    if k8s_cluster:
        ns = namespace or "UnknownNamespace"
        prefix = "eks" if cloud_platform == "aws_eks" else "k8s"
        return f"{prefix}:{k8s_cluster}/{ns}"

    # 3. ECS: "ecs:<cluster>" — cluster name is the last segment of the ECS cluster ARN.
    ecs_cluster_arn = _str("aws.ecs.cluster.arn")
    if ecs_cluster_arn:
        ecs_cluster = ecs_cluster_arn.rsplit("/", maxsplit=1)[-1]
        if ecs_cluster:
            return f"ecs:{ecs_cluster}"

    # 4. EC2: only when the host is actually EC2 (matches the agent's Platform == ModeEC2
    #    gate). Signals that the host is EC2: cloud.platform=aws_ec2, host.id (EC2 instance
    #    id from the OTel EC2 detector), or the ASG tag (an IMDS-only EC2 signal). Accept any
    #    so we still resolve ec2:* when cloud.platform wasn't populated.
    asg = _str("ec2.tag.aws:autoscaling:groupName")
    if not asg and asg_supplier is not None:
        asg = (asg_supplier() or "").strip()
    is_ec2 = cloud_platform == "aws_ec2" or bool(_str("host.id")) or bool(asg)
    if is_ec2:
        return f"ec2:{asg}" if asg else "ec2:default"

    # 5. Non-AWS / undetected host: the CloudWatch agent runs its "generic" resolver here and
    #    emits "generic:default" (never empty), so mirror that instead of omitting the key.
    return "generic:default"


def has_platform_context(attributes: Mapping[str, object]) -> bool:
    """Return True if the attributes carry enough platform context for a fallback value to be real.

    ``ec2:default`` and ``generic:default`` are also what a half-populated resource momentarily
    yields before detectors settle, so callers that cache the resolved value use this to avoid
    caching a fallback prematurely. A specific value (eks:/k8s:/ecs:/ec2:<asg>/explicit env) is
    always safe to cache.
    """
    return any(attributes.get(key) for key in _PLATFORM_CONTEXT_KEYS)


def stamp_local_environment(attrs: Dict[str, str]) -> None:
    """Stamp aws.local.environment onto a mutable attribute dict (idempotent).

    The resolver always yields a non-empty value (a platform scope, an explicit env, or the
    "generic:default" fallback), matching the CloudWatch agent, which always sets Environment."""
    if attrs.get(AWS_LOCAL_ENVIRONMENT_KEY):
        return
    resolved = resolve_local_environment(attrs)
    if resolved:
        attrs[AWS_LOCAL_ENVIRONMENT_KEY] = resolved
