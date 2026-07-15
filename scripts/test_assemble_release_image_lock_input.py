#!/usr/bin/env python3

import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

import assemble_release_image_lock_input as assembler


SCRIPT = Path(__file__).with_name("assemble_release_image_lock_input.py")
BUILDER = Path(__file__).with_name("build_release_image_lock.py")
HEAD_SHA = "a" * 40


def digest(character):
    return "sha256:" + character * 64


def canonical(value):
    return json.dumps(
        value, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")


def artifact(component, character="1"):
    repository = f"registry.example/fugue-{component.replace('_', '-')}"
    top_digest = digest(character)
    return {
        "component": component,
        "repository": repository,
        "source_tag": HEAD_SHA,
        "top_digest": top_digest,
        "platform_manifest_digest": digest("b"),
        "config_digest": digest("c"),
        "oci_revision": HEAD_SHA,
        "immutable_ref": f"{repository}@{top_digest}",
        "verification": "registry_manifest_config_and_layer_get",
    }


WORKLOADS = {
    "api": ("api.image", "Deployment", "fugue-fugue-api", "api"),
    "controller": (
        "controller.image",
        "Deployment",
        "fugue-fugue-controller",
        "controller",
    ),
    "drain_agent": (
        "runtime.strictDrain.agent.image",
        "Configuration",
        "fugue-fugue-controller",
        "controller",
    ),
    "telemetry_agent": (
        "observability.agent.image",
        "Deployment",
        "fugue-fugue-telemetry-agent",
        "telemetry-agent",
    ),
    "image_cache": (
        "imageCache.image",
        "DaemonSet",
        "fugue-fugue-image-cache",
        "image-cache",
    ),
    "edge": ("edge.image", "DaemonSet", "fugue-fugue-edge", "edge"),
}


def built_activation(source_artifact, *, helm_path=None, workload=None):
    component = source_artifact["component"]
    default_path, kind, name, container = WORKLOADS[component]
    if workload is not None:
        kind, name, container = workload
    return {
        "component": component,
        "source_mode": "built",
        "source_template_ref": (
            f"{source_artifact['repository']}:{source_artifact['source_tag']}"
        ),
        "selected_ref": source_artifact["immutable_ref"],
        "repository": source_artifact["repository"],
        "source_tag": source_artifact["source_tag"],
        "digest": source_artifact["top_digest"],
        "runtime_manifest_digest": source_artifact["platform_manifest_digest"],
        "pin_state": "pinned",
        "migration_allowed": False,
        "helm_path": helm_path or default_path,
        "workload": {"kind": kind, "name": name, "container": container},
    }


def preserve_activation(
    component="api",
    *,
    helm_path=None,
    workload=None,
    character="7",
):
    default_path, kind, name, container = WORKLOADS[component]
    if workload is not None:
        kind, name, container = workload
    repository = f"registry.example/live-{component.replace('_', '-')}"
    image_digest = digest(character)
    return {
        "component": component,
        "source_mode": "preserve",
        "source_template_ref": f"{repository}@{image_digest}",
        "selected_ref": f"{repository}@{image_digest}",
        "repository": repository,
        "source_tag": "live-v1",
        "digest": image_digest,
        "runtime_manifest_digest": digest("8"),
        "pin_state": "pinned",
        "migration_allowed": False,
        "helm_path": helm_path or default_path,
        "workload": {"kind": kind, "name": name, "container": container},
    }


class AssembleReleaseImageLockInputTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary.name)
        os.chmod(self.directory, 0o700)
        self.counter = 0

    def tearDown(self):
        self.temporary.cleanup()

    def write_private(self, name, payload):
        path = self.directory / name
        path.write_bytes(payload)
        os.chmod(path, 0o600)
        return path

    def invoke_paths(
        self,
        artifacts_path,
        activations_path,
        *,
        expected_digest,
        expected_components,
        expected_count=None,
        output_path=None,
        timeout=5,
        extra_args=(),
    ):
        self.counter += 1
        if output_path is None:
            output_path = self.directory / f"assembled-{self.counter}.json"
        if expected_count is None:
            expected_count = "0" if expected_components == "" else str(
                len(expected_components.split(" "))
            )
        command = [
            "python3",
            str(SCRIPT),
            "--verified-artifacts",
            str(artifacts_path),
            "--expected-verified-artifacts-digest",
            expected_digest,
            "--expected-built-components",
            expected_components,
            "--expected-built-component-count",
            expected_count,
            "--activations",
            str(activations_path),
            "--repository",
            "acme/fugue",
            "--run-id",
            "123",
            "--run-attempt",
            "1",
            "--head-sha",
            HEAD_SHA,
            "--output",
            str(output_path),
            *extra_args,
        ]
        result = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
        return result, output_path

    def invoke(
        self,
        artifacts,
        activations,
        *,
        expected_components=None,
        expected_count=None,
        artifacts_raw=None,
        activations_raw=None,
        expected_digest=None,
        extra_args=(),
    ):
        self.counter += 1
        artifact_payload = canonical(artifacts) if artifacts_raw is None else artifacts_raw
        activation_payload = canonical(activations) if activations_raw is None else activations_raw
        artifacts_path = self.write_private(f"artifacts-{self.counter}.json", artifact_payload)
        activations_path = self.write_private(f"activations-{self.counter}.json", activation_payload)
        if expected_components is None:
            expected_components = " ".join(item["component"] for item in artifacts)
        if expected_digest is None:
            expected_digest = "sha256:" + hashlib.sha256(artifact_payload).hexdigest()
        return self.invoke_paths(
            artifacts_path,
            activations_path,
            expected_digest=expected_digest,
            expected_components=expected_components,
            expected_count=expected_count,
            extra_args=extra_args,
        )

    def load_output(self, path):
        return json.loads(path.read_text(encoding="utf-8"))

    def assert_builder_accepts(self, source_path):
        self.counter += 1
        output = self.directory / f"lock-{self.counter}.json"
        result = subprocess.run(
            ["python3", str(BUILDER), "--input", str(source_path), "--output", str(output)],
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(output.read_text(encoding="utf-8"))

    def test_disabled_built_components_are_excluded_without_weakening_builder(self):
        for component in ("telemetry_agent", "image_cache", "edge"):
            with self.subTest(component=component):
                source_artifact = artifact(component)
                result, output = self.invoke(
                    [source_artifact],
                    [preserve_activation()],
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(self.load_output(output)["artifacts"], [])
                lock = self.assert_builder_accepts(output)
                self.assertEqual(lock["artifacts"], [])

    def test_edge_artifact_is_retained_for_dns_or_mesh_consumer(self):
        cases = (
            ("dns.image", ("DaemonSet", "fugue-fugue-dns", "dns")),
            (
                "meshRecovery.image",
                ("DaemonSet", "fugue-fugue-mesh-recovery", "mesh-recovery"),
            ),
        )
        for helm_path, workload in cases:
            with self.subTest(helm_path=helm_path):
                edge_artifact = artifact("edge", "2")
                activation = built_activation(
                    edge_artifact, helm_path=helm_path, workload=workload
                )
                result, output = self.invoke([edge_artifact], [activation])
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(
                    [item["component"] for item in self.load_output(output)["artifacts"]],
                    ["edge"],
                )
                self.assert_builder_accepts(output)

    def test_preserved_edge_cohort_excludes_unused_new_artifact(self):
        edge_artifact = artifact("edge", "2")
        result, output = self.invoke(
            [edge_artifact],
            [preserve_activation("edge")],
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.load_output(output)["artifacts"], [])
        self.assert_builder_accepts(output)

    def test_mixed_edge_cohort_retains_one_artifact(self):
        edge_artifact = artifact("edge", "2")
        activations = [
            built_activation(edge_artifact),
            preserve_activation(
                "edge",
                helm_path="dns.image",
                workload=("DaemonSet", "fugue-fugue-dns", "dns"),
            ),
        ]
        result, output = self.invoke([edge_artifact], activations)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            [item["component"] for item in self.load_output(output)["artifacts"]],
            ["edge"],
        )
        self.assert_builder_accepts(output)

    def test_app_ssh_is_the_only_supported_artifact_only_component(self):
        app_ssh = artifact("app_ssh", "3")
        result, output = self.invoke([app_ssh], [preserve_activation()])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            [item["component"] for item in self.load_output(output)["artifacts"]],
            ["app_ssh"],
        )
        self.assert_builder_accepts(output)

        invalid = preserve_activation()
        invalid["component"] = "app_ssh"
        result, output = self.invoke([app_ssh], [invalid])
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(output.exists())

    def test_preserve_only_release_requires_authenticated_canonical_empty_provenance(self):
        result, output = self.invoke([], [preserve_activation()])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.load_output(output)["artifacts"], [])
        self.assert_builder_accepts(output)

        result, output = self.invoke(
            [],
            [preserve_activation()],
            expected_digest="sha256:" + "0" * 64,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(output.exists())

    def test_built_activation_requires_exact_verified_artifact_identity(self):
        api_artifact = artifact("api", "1")
        activation = built_activation(api_artifact)
        result, output = self.invoke([], [activation], expected_components="")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(output.exists())

        for field, value in (
            ("top_digest", digest("4")),
            ("platform_manifest_digest", digest("5")),
        ):
            with self.subTest(field=field):
                changed_artifact = dict(api_artifact)
                changed_artifact[field] = value
                if field == "top_digest":
                    changed_artifact["immutable_ref"] = (
                        f"{changed_artifact['repository']}@{value}"
                    )
                result, output = self.invoke([changed_artifact], [activation])
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(output.exists())

    def test_current_build_identity_cannot_be_downgraded_to_preserve_or_migration(self):
        api_artifact = artifact("api", "1")
        exact = built_activation(api_artifact)

        preserve = dict(exact)
        preserve.update(
            {
                "source_mode": "preserve",
                "source_template_ref": api_artifact["immutable_ref"],
            }
        )
        migration = dict(exact)
        migration.update(
            {
                "source_mode": "migration",
                "source_template_ref": (
                    f"{api_artifact['repository']}:{api_artifact['source_tag']}"
                ),
                "migration_allowed": True,
            }
        )
        legacy_tag = dict(exact)
        legacy_tag.update(
            {
                "source_mode": "preserve",
                "source_template_ref": (
                    f"{api_artifact['repository']}:{api_artifact['source_tag']}"
                ),
                "selected_ref": (
                    f"{api_artifact['repository']}:{api_artifact['source_tag']}"
                ),
                "digest": "",
                "runtime_manifest_digest": "",
                "pin_state": "legacy_unpinned",
                "migration_allowed": True,
            }
        )
        fake_old_tag = dict(preserve)
        fake_old_tag["source_tag"] = "old-v1"
        for source_mode, activation in (
            ("preserve-exact", preserve),
            ("migration-exact", migration),
            ("preserve-tag-only", legacy_tag),
            ("preserve-exact-digest-fake-old-tag", fake_old_tag),
        ):
            with self.subTest(source_mode=source_mode):
                result, output = self.invoke([api_artifact], [activation])
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(output.exists())

        old_tag = preserve_activation()
        old_tag["repository"] = api_artifact["repository"]
        old_tag["source_template_ref"] = (
            f"{api_artifact['repository']}@{old_tag['digest']}"
        )
        old_tag["selected_ref"] = old_tag["source_template_ref"]
        result, output = self.invoke([api_artifact], [old_tag])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.load_output(output)["artifacts"], [])
        self.assert_builder_accepts(output)

        cross_component = preserve_activation("controller")
        cross_component.update(
            {
                "repository": api_artifact["repository"],
                "source_tag": "old-v1",
                "digest": api_artifact["top_digest"],
                "runtime_manifest_digest": api_artifact[
                    "platform_manifest_digest"
                ],
                "source_template_ref": api_artifact["immutable_ref"],
                "selected_ref": api_artifact["immutable_ref"],
            }
        )
        result, output = self.invoke([api_artifact], [cross_component])
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(output.exists())

        platform_manifest_alias = preserve_activation()
        platform_manifest_alias.update(
            {
                "repository": api_artifact["repository"],
                "source_tag": "old-v1",
                "digest": api_artifact["platform_manifest_digest"],
                "runtime_manifest_digest": api_artifact[
                    "platform_manifest_digest"
                ],
                "source_template_ref": (
                    f"{api_artifact['repository']}@"
                    f"{api_artifact['platform_manifest_digest']}"
                ),
                "selected_ref": (
                    f"{api_artifact['repository']}@"
                    f"{api_artifact['platform_manifest_digest']}"
                ),
            }
        )
        result, output = self.invoke([api_artifact], [platform_manifest_alias])
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(output.exists())

        runtime_manifest_alias = preserve_activation()
        runtime_manifest_alias.update(
            {
                "repository": api_artifact["repository"],
                "source_tag": "old-v1",
                "digest": digest("d"),
                "runtime_manifest_digest": api_artifact[
                    "platform_manifest_digest"
                ],
                "source_template_ref": f"{api_artifact['repository']}@{digest('d')}",
                "selected_ref": f"{api_artifact['repository']}@{digest('d')}",
            }
        )
        result, output = self.invoke([api_artifact], [runtime_manifest_alias])
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(output.exists())

    def test_full_provenance_schema_order_digest_and_build_plan_are_strict(self):
        api_artifact = artifact("api", "1")
        controller_artifact = artifact("controller", "2")
        activation = built_activation(api_artifact)

        cases = []
        duplicate = [api_artifact, dict(api_artifact)]
        cases.append((duplicate, None, None, None, "duplicate"))
        unknown = [dict(api_artifact, component="unknown")]
        cases.append((unknown, None, None, None, "unknown"))
        cases.append(
            (
                [controller_artifact, api_artifact],
                "api controller",
                None,
                None,
                "out-of-order",
            )
        )
        cases.append(
            (
                [api_artifact],
                "controller",
                None,
                None,
                "plan mismatch",
            )
        )
        cases.append(
            (
                [api_artifact],
                "api",
                "2",
                None,
                "count mismatch",
            )
        )
        cases.append(
            (
                [api_artifact],
                "api",
                None,
                "sha256:" + "0" * 64,
                "digest mismatch",
            )
        )
        for artifacts, components, count, expected_digest, label in cases:
            with self.subTest(label=label):
                result, output = self.invoke(
                    artifacts,
                    [activation],
                    expected_components=components,
                    expected_count=count,
                    expected_digest=expected_digest,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(output.exists())

        noncanonical = json.dumps([api_artifact], sort_keys=True, indent=2).encode("utf-8")
        result, output = self.invoke(
            [api_artifact],
            [activation],
            artifacts_raw=noncanonical,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(output.exists())

        for numeric_payload in (b"[1e999]", b"[" + b"9" * 5000 + b"]"):
            with self.subTest(numeric_payload=numeric_payload[:20]):
                result, output = self.invoke(
                    [],
                    [preserve_activation()],
                    artifacts_raw=numeric_payload,
                    expected_components="",
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertNotIn("Traceback", result.stderr)
                self.assertFalse(output.exists())

        deeply_nested = b"[" * 1000 + b"null" + b"]" * 1000
        result, output = self.invoke(
            [],
            [preserve_activation()],
            artifacts_raw=deeply_nested,
            expected_components="",
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("Traceback", result.stderr)
        self.assertFalse(output.exists())

        result, output = self.invoke(
            [],
            [preserve_activation()],
            expected_components="",
            expected_count="9" * 5000,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("Traceback", result.stderr)
        self.assertFalse(output.exists())

    def test_producer_and_build_plan_canonical_orders_are_bound_by_component_set(self):
        producer_order = [
            artifact("api", "1"),
            artifact("app_ssh", "2"),
            artifact("controller", "3"),
            artifact("drain_agent", "4"),
            artifact("edge", "5"),
            artifact("image_cache", "6"),
            artifact("telemetry_agent", "7"),
        ]
        plan_order = (
            "api controller drain_agent telemetry_agent image_cache edge app_ssh"
        )
        result, output = self.invoke(
            producer_order,
            [preserve_activation()],
            expected_components=plan_order,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            [item["component"] for item in self.load_output(output)["artifacts"]],
            ["app_ssh"],
        )
        self.assert_builder_accepts(output)

    def test_secure_input_rejects_symlink_hardlink_fifo_and_public_parent(self):
        artifacts = canonical([])
        activations = canonical([preserve_activation()])
        digest_value = "sha256:" + hashlib.sha256(artifacts).hexdigest()
        real_artifacts = self.write_private("secure-artifacts.json", artifacts)
        activation_path = self.write_private("secure-activations.json", activations)

        symlink_path = self.directory / "artifact-symlink.json"
        symlink_path.symlink_to(real_artifacts)
        result, output = self.invoke_paths(
            symlink_path,
            activation_path,
            expected_digest=digest_value,
            expected_components="",
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(output.exists())

        hardlink_path = self.directory / "artifact-hardlink.json"
        os.link(real_artifacts, hardlink_path)
        result, output = self.invoke_paths(
            hardlink_path,
            activation_path,
            expected_digest=digest_value,
            expected_components="",
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(output.exists())
        hardlink_path.unlink()

        fifo_path = self.directory / "artifact-fifo.json"
        os.mkfifo(fifo_path, 0o600)
        result, output = self.invoke_paths(
            fifo_path,
            activation_path,
            expected_digest=digest_value,
            expected_components="",
            timeout=2,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(output.exists())

        public_directory = self.directory / "public"
        public_directory.mkdir(mode=0o770)
        public_artifacts = public_directory / "artifacts.json"
        public_artifacts.write_bytes(artifacts)
        os.chmod(public_artifacts, 0o600)
        public_activations = public_directory / "activations.json"
        public_activations.write_bytes(activations)
        os.chmod(public_activations, 0o600)
        result, output = self.invoke_paths(
            public_artifacts,
            public_activations,
            expected_digest=digest_value,
            expected_components="",
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(output.exists())

    def test_secure_reader_detects_same_size_mutation_during_read(self):
        original = canonical([])
        changed = b"{}"
        self.assertEqual(len(original), len(changed))
        path = self.write_private("race.json", original)
        real_read = assembler._read_fd

        def mutate_after_read(file_fd, limit):
            payload = real_read(file_fd, limit)
            writer = os.open(path, os.O_WRONLY)
            try:
                os.write(writer, changed)
                os.fsync(writer)
            finally:
                os.close(writer)
            return payload

        with mock.patch.object(assembler, "_read_fd", side_effect=mutate_after_read):
            with self.assertRaisesRegex(
                assembler.LockBuildError, "changed while it was read"
            ):
                assembler.read_secure_input(path, "race input")

    def test_output_is_write_once_and_deterministic(self):
        artifacts = [artifact("api", "1"), artifact("app_ssh", "3")]
        activations = [built_activation(artifacts[0])]
        first_result, first_output = self.invoke(artifacts, activations)
        second_result, second_output = self.invoke(artifacts, activations)
        self.assertEqual(first_result.returncode, 0, first_result.stderr)
        self.assertEqual(second_result.returncode, 0, second_result.stderr)
        self.assertEqual(first_output.read_bytes(), second_output.read_bytes())
        self.assert_builder_accepts(first_output)

        artifact_payload = canonical(artifacts)
        activation_payload = canonical(activations)
        artifacts_path = self.write_private("existing-artifacts.json", artifact_payload)
        activations_path = self.write_private("existing-activations.json", activation_payload)
        existing_output = self.write_private("already-exists.json", b"sentinel")
        result, _ = self.invoke_paths(
            artifacts_path,
            activations_path,
            expected_digest="sha256:" + hashlib.sha256(artifact_payload).hexdigest(),
            expected_components="api app_ssh",
            output_path=existing_output,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(existing_output.read_bytes(), b"sentinel")

    def test_duplicate_cli_option_is_rejected(self):
        result, output = self.invoke(
            [],
            [preserve_activation()],
            extra_args=("--run-id", "456"),
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
