# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from unittest import TestCase

from amazon.opentelemetry.distro.serviceevents.utils.environment_resolver import (
    AWS_LOCAL_ENVIRONMENT_KEY,
    resolve_local_environment,
    stamp_local_environment,
)


class TestResolveLocalEnvironment(TestCase):
    """The SDK-side resolver must produce the SAME aws.local.environment the CloudWatch
    agent's awsapplicationsignals resolver produces, from the same OTel resource attributes.
    Precedence: explicit deployment.environment[.name] -> eks/k8s cluster/namespace ->
    ecs:<cluster> -> ec2:<asg> -> ec2:default.
    """

    def test_explicit_environment_name_wins(self):
        self.assertEqual(
            resolve_local_environment(
                {
                    "deployment.environment.name": "my-env",
                    "k8s.cluster.name": "c",
                    "k8s.namespace.name": "ns",
                    "cloud.platform": "aws_eks",
                    "ec2.tag.aws:autoscaling:groupName": "asg",
                }
            ),
            "my-env",
        )

    def test_legacy_deployment_environment_honored(self):
        self.assertEqual(
            resolve_local_environment({"deployment.environment": "legacy-env"}),
            "legacy-env",
        )

    def test_environment_name_preferred_over_legacy(self):
        self.assertEqual(
            resolve_local_environment({"deployment.environment.name": "new", "deployment.environment": "legacy"}),
            "new",
        )

    def test_eks_cluster_namespace(self):
        self.assertEqual(
            resolve_local_environment(
                {
                    "cloud.platform": "aws_eks",
                    "k8s.cluster.name": "my-cluster",
                    "k8s.namespace.name": "default",
                }
            ),
            "eks:my-cluster/default",
        )

    def test_eks_missing_namespace_falls_back_to_unknown(self):
        self.assertEqual(
            resolve_local_environment({"cloud.platform": "aws_eks", "k8s.cluster.name": "my-cluster"}),
            "eks:my-cluster/UnknownNamespace",
        )

    def test_non_eks_kubernetes_uses_k8s_prefix(self):
        self.assertEqual(
            resolve_local_environment({"k8s.cluster.name": "c", "k8s.namespace.name": "team-a"}),
            "k8s:c/team-a",
        )

    def test_ecs_cluster_from_arn(self):
        self.assertEqual(
            resolve_local_environment(
                {
                    "cloud.platform": "aws_ecs",
                    "aws.ecs.cluster.arn": "arn:aws:ecs:us-west-2:123456789012:cluster/my-ecs-cluster",
                }
            ),
            "ecs:my-ecs-cluster",
        )

    def test_explicit_environment_wins_over_ecs(self):
        self.assertEqual(
            resolve_local_environment(
                {
                    "deployment.environment.name": "prod",
                    "cloud.platform": "aws_ecs",
                    "aws.ecs.cluster.arn": "arn:aws:ecs:us-west-2:123456789012:cluster/my-ecs-cluster",
                }
            ),
            "prod",
        )

    def test_ec2_with_asg(self):
        self.assertEqual(
            resolve_local_environment({"ec2.tag.aws:autoscaling:groupName": "my-asg"}),
            "ec2:my-asg",
        )

    def test_ec2_without_asg_defaults(self):
        self.assertEqual(resolve_local_environment({"cloud.platform": "aws_ec2"}), "ec2:default")

    def test_empty_attributes_non_aws_returns_generic_default(self):
        # No platform signal (non-AWS / undetected host): the agent runs its "generic" resolver
        # and emits "generic:default", so the SDK matches that rather than claiming ec2:default.
        self.assertEqual(resolve_local_environment({}), "generic:default")

    def test_non_aws_host_with_only_service_name_returns_generic_default(self):
        self.assertEqual(resolve_local_environment({"service.name": "svc", "host.name": "my-vm"}), "generic:default")

    def test_ec2_default_when_host_id_present(self):
        # host.id (EC2 instance id from the OTel EC2 detector) marks the host as EC2.
        self.assertEqual(resolve_local_environment({"cloud.platform": "aws_ec2", "host.id": "i-0abc"}), "ec2:default")

    def test_kubernetes_precedes_ecs_and_ec2(self):
        # When both k8s and ec2 ASG are present (unusual), Kubernetes wins, matching the agent.
        self.assertEqual(
            resolve_local_environment(
                {
                    "k8s.cluster.name": "c",
                    "k8s.namespace.name": "ns",
                    "ec2.tag.aws:autoscaling:groupName": "asg",
                }
            ),
            "k8s:c/ns",
        )

    def test_whitespace_only_values_ignored(self):
        # Blank/whitespace explicit env must not win; falls through to platform scope.
        self.assertEqual(
            resolve_local_environment(
                {
                    "deployment.environment.name": "   ",
                    "cloud.platform": "aws_eks",
                    "k8s.cluster.name": "my-cluster",
                    "k8s.namespace.name": "default",
                }
            ),
            "eks:my-cluster/default",
        )

    def test_ecs_arn_without_slash_uses_whole_value(self):
        # No slash -> the whole value is the last segment; still non-empty, so "ecs:<value>".
        self.assertEqual(
            resolve_local_environment({"cloud.platform": "aws_ecs", "aws.ecs.cluster.arn": "bogus"}),
            "ecs:bogus",
        )

    def test_ecs_empty_arn_falls_through_to_generic_default(self):
        # A trailing-slash ARN yields an empty cluster name; with cloud.platform=aws_ecs (not
        # aws_ec2) and no EC2 signal, the resolver falls through to "generic:default" (not
        # ec2:default), matching the agent's generic resolver.
        self.assertEqual(
            resolve_local_environment({"cloud.platform": "aws_ecs", "aws.ecs.cluster.arn": "arn:.../cluster/"}),
            "generic:default",
        )


class TestStampLocalEnvironment(TestCase):
    def test_stamps_resolved_value(self):
        attrs = {
            "cloud.platform": "aws_eks",
            "k8s.cluster.name": "my-cluster",
            "k8s.namespace.name": "default",
        }
        stamp_local_environment(attrs)
        self.assertEqual(attrs[AWS_LOCAL_ENVIRONMENT_KEY], "eks:my-cluster/default")

    def test_does_not_overwrite_existing(self):
        attrs = {
            AWS_LOCAL_ENVIRONMENT_KEY: "already-set",
            "cloud.platform": "aws_eks",
            "k8s.cluster.name": "c",
            "k8s.namespace.name": "ns",
        }
        stamp_local_environment(attrs)
        self.assertEqual(attrs[AWS_LOCAL_ENVIRONMENT_KEY], "already-set")
