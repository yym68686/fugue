#!/usr/bin/env python3

import copy
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import unittest


REPO_ROOT = Path(__file__).resolve().parent.parent
BUILDER = REPO_ROOT / "scripts" / "build_release_image_lock.py"
HEAD_SHA = "a" * 40
VERIFICATION = "registry_manifest_config_and_layer_get"


def digest(character):
    return "sha256:" + character * 64


def artifact(component, repository, marker):
    top_digest = digest(marker)
    return {
        "component": component,
        "repository": repository,
        "source_tag": HEAD_SHA,
        "top_digest": top_digest,
        "platform_manifest_digest": digest(chr(ord(marker) + 1)),
        "config_digest": digest(chr(ord(marker) + 2)),
        "oci_revision": HEAD_SHA,
        "immutable_ref": f"{repository}@{top_digest}",
        "verification": VERIFICATION,
    }


def built_activation(source_artifact, helm_path="api.image", name="fugue-api"):
    repository = source_artifact["repository"]
    return {
        "component": source_artifact["component"],
        "source_mode": "built",
        "source_template_ref": f"{repository}:{HEAD_SHA}",
        "selected_ref": source_artifact["immutable_ref"],
        "repository": repository,
        "source_tag": HEAD_SHA,
        "digest": source_artifact["top_digest"],
        "runtime_manifest_digest": source_artifact["platform_manifest_digest"],
        "pin_state": "pinned",
        "migration_allowed": False,
        "helm_path": helm_path,
        "workload": {"kind": "Deployment", "name": name, "container": "api"},
    }


def pinned_preserve_activation():
    repository = "ghcr.io/acme/fugue-controller"
    image_digest = digest("7")
    return {
        "component": "controller",
        "source_mode": "preserve",
        "source_template_ref": f"{repository}:stable@{image_digest}",
        "selected_ref": f"{repository}@{image_digest}",
        "repository": repository,
        "source_tag": "stable",
        "digest": image_digest,
        "runtime_manifest_digest": digest("8"),
        "pin_state": "pinned",
        "migration_allowed": False,
        "helm_path": "controller.image",
        "workload": {
            "kind": "Deployment",
            "name": "fugue-controller",
            "container": "controller",
        },
    }


def legacy_preserve_activation():
    repository = "ghcr.io/acme/fugue-edge"
    return {
        "component": "edge",
        "source_mode": "preserve",
        "source_template_ref": f"{repository}:legacy",
        "selected_ref": f"{repository}:legacy",
        "repository": repository,
        "source_tag": "legacy",
        "digest": "",
        "runtime_manifest_digest": "",
        "pin_state": "legacy_unpinned",
        "migration_allowed": True,
        "helm_path": "edge.groups[0].blueGreen.slots.a.image",
        "workload": {"kind": "DaemonSet", "name": "fugue-edge-a", "container": "edge"},
    }


def valid_document():
    api_artifact = artifact("api", "ghcr.io/acme/fugue-api", "1")
    ssh_artifact = artifact("app_ssh", "ghcr.io/acme/fugue-app-ssh", "4")
    return {
        "schema_version": 1,
        "release": {
            "repository": "acme/fugue",
            "workflow": "deploy-control-plane",
            "run_id": "123456789",
            "run_attempt": "2",
            "head_sha": HEAD_SHA,
            "platform": "linux/amd64",
        },
        "artifacts": [ssh_artifact, api_artifact],
        "activations": [pinned_preserve_activation(), built_activation(api_artifact)],
    }


class BuilderTestCase(unittest.TestCase):
    def invoke(
        self,
        document=None,
        raw=None,
        output_setup=None,
        output_override=None,
        child_umask=None,
    ):
        temporary = tempfile.TemporaryDirectory(prefix="fugue-lock-test-")
        self.addCleanup(temporary.cleanup)
        directory = Path(temporary.name)
        os.chmod(directory, 0o700)
        input_path = directory / "input.json"
        if raw is None:
            raw = json.dumps(document, separators=(",", ":"), sort_keys=True).encode()
        elif isinstance(raw, str):
            raw = raw.encode()
        input_path.write_bytes(raw)
        output_path = directory / "release-image-lock.json"
        if output_setup is not None:
            output_setup(output_path)
        command_output = str(output_path) if output_override is None else output_override
        result = subprocess.run(
            [
                sys.executable,
                str(BUILDER),
                "--input",
                str(input_path),
                "--output",
                command_output,
            ],
            cwd=REPO_ROOT,
            check=False,
            capture_output=True,
            text=True,
            preexec_fn=(lambda: os.umask(child_umask)) if child_umask is not None else None,
        )
        return result, directory, output_path

    def assert_validation_failure(self, document=None, raw=None):
        result, directory, output_path = self.invoke(document=document, raw=raw)
        self.assertNotEqual(result.returncode, 0, result.stderr)
        self.assertFalse(output_path.exists())
        self.assertEqual(list(directory.glob(".release-image-lock.json.tmp.*")), [])
        return result


class ReleaseImageLockBuilderTests(BuilderTestCase):
    def test_valid_built_preserve_and_app_ssh_artifact_only(self):
        document = valid_document()
        result, _, output_path = self.invoke(document=document)
        self.assertEqual(result.returncode, 0, result.stderr)
        output = json.loads(output_path.read_text())
        self.assertEqual(output["schema_version"], 1)
        self.assertEqual(output["producer"], "fugue-release-image-lock")
        self.assertEqual(
            [item["component"] for item in output["artifacts"]], ["api", "app_ssh"]
        )
        self.assertEqual(
            [item["component"] for item in output["activations"]], ["api", "controller"]
        )
        built = output["activations"][0]
        self.assertEqual(built["selected_ref"], document["artifacts"][1]["immutable_ref"])
        # A digest-only ref keeps its exact form; the hint supplies only source-tag semantics.
        self.assertNotIn(":" + HEAD_SHA + "@", built["selected_ref"])
        claimed_digest = output.pop("lock_digest")
        canonical = json.dumps(
            output,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        self.assertEqual(claimed_digest, "sha256:" + hashlib.sha256(canonical).hexdigest())
        self.assertTrue(output_path.read_bytes().endswith(b"\n"))

    def test_output_is_deterministic_across_input_array_order(self):
        first = valid_document()
        second = copy.deepcopy(first)
        second["artifacts"].reverse()
        second["activations"].reverse()
        first_result, _, first_output = self.invoke(document=first)
        second_result, _, second_output = self.invoke(document=second)
        self.assertEqual(first_result.returncode, 0, first_result.stderr)
        self.assertEqual(second_result.returncode, 0, second_result.stderr)
        self.assertEqual(first_output.read_bytes(), second_output.read_bytes())

    def test_output_mode_is_exactly_0600(self):
        result, _, output_path = self.invoke(document=valid_document(), child_umask=0o777)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(stat.S_IMODE(output_path.stat().st_mode), 0o600)

    def test_existing_output_is_untouched_and_has_no_temporary_residue(self):
        original = b"do-not-replace\n"

        def create_existing(path):
            path.write_bytes(original)

        result, directory, output_path = self.invoke(
            document=valid_document(), output_setup=create_existing
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(output_path.read_bytes(), original)
        self.assertEqual(list(directory.glob(".release-image-lock.json.tmp.*")), [])

    def test_concurrent_publish_has_exactly_one_winner(self):
        with tempfile.TemporaryDirectory(prefix="fugue-lock-race-") as directory_name:
            directory = Path(directory_name)
            os.chmod(directory, 0o700)
            input_path = directory / "input.json"
            input_path.write_text(
                json.dumps(valid_document(), separators=(",", ":"), sort_keys=True)
            )
            output_path = directory / "release-image-lock.json"
            command = [
                sys.executable,
                str(BUILDER),
                "--input",
                str(input_path),
                "--output",
                str(output_path),
            ]
            processes = [
                subprocess.Popen(
                    command,
                    cwd=REPO_ROOT,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                for _ in range(12)
            ]
            results = [
                process.communicate(timeout=10) + (process.returncode,)
                for process in processes
            ]
            self.assertEqual(sum(returncode == 0 for _, _, returncode in results), 1)
            self.assertEqual(stat.S_IMODE(output_path.stat().st_mode), 0o600)
            self.assertEqual(
                json.loads(output_path.read_text())["producer"], "fugue-release-image-lock"
            )
            self.assertEqual(list(directory.glob(".release-image-lock.json.tmp.*")), [])

    def test_existing_output_symlink_is_untouched(self):
        target_holder = {}

        def create_symlink(path):
            target = path.parent / "symlink-target"
            target.write_bytes(b"original")
            path.symlink_to(target)
            target_holder["path"] = target

        result, directory, output_path = self.invoke(
            document=valid_document(), output_setup=create_symlink
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertTrue(output_path.is_symlink())
        self.assertEqual(target_holder["path"].read_bytes(), b"original")
        self.assertEqual(list(directory.glob(".release-image-lock.json.tmp.*")), [])

    def test_symlink_parent_is_rejected(self):
        with tempfile.TemporaryDirectory(prefix="fugue-lock-parent-") as outer_name:
            outer = Path(outer_name)
            os.chmod(outer, 0o700)
            real_parent = outer / "real"
            real_parent.mkdir(mode=0o700)
            link_parent = outer / "linked"
            link_parent.symlink_to(real_parent, target_is_directory=True)
            result, _, _ = self.invoke(
                document=valid_document(),
                output_override=str(link_parent / "release-image-lock.json"),
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse((real_parent / "release-image-lock.json").exists())
            self.assertEqual(list(real_parent.iterdir()), [])

    def test_group_writable_parent_is_rejected(self):
        result, directory, output_path = self.invoke(document=valid_document())
        # The first invocation proves the fixture itself is valid; use a separate parent below.
        self.assertEqual(result.returncode, 0, result.stderr)
        output_path.unlink()
        os.chmod(directory, 0o770)
        second = subprocess.run(
            [
                sys.executable,
                str(BUILDER),
                "--input",
                str(directory / "input.json"),
                "--output",
                str(output_path),
            ],
            cwd=REPO_ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(second.returncode, 0)
        self.assertFalse(output_path.exists())
        self.assertEqual(list(directory.glob(".release-image-lock.json.tmp.*")), [])

    def test_relative_output_is_rejected_without_creating_it(self):
        relative = f"release-image-lock-{os.getpid()}-{id(self)}.json"
        result, _, _ = self.invoke(document=valid_document(), output_override=relative)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((REPO_ROOT / relative).exists())

    def test_unknown_fields_are_rejected_at_every_schema_level(self):
        mutations = []
        root = valid_document()
        root["extra"] = True
        mutations.append(root)
        release = valid_document()
        release["release"]["extra"] = True
        mutations.append(release)
        artifact_value = valid_document()
        artifact_value["artifacts"][0]["extra"] = True
        mutations.append(artifact_value)
        activation = valid_document()
        activation["activations"][0]["extra"] = True
        mutations.append(activation)
        workload = valid_document()
        workload["activations"][0]["workload"]["extra"] = True
        mutations.append(workload)
        for index, document in enumerate(mutations):
            with self.subTest(index=index):
                self.assert_validation_failure(document=document)

    def test_missing_required_fields_are_rejected(self):
        for path in ("release", "artifact", "activation", "workload"):
            document = valid_document()
            if path == "release":
                del document["release"]["run_id"]
            elif path == "artifact":
                del document["artifacts"][0]["config_digest"]
            elif path == "activation":
                del document["activations"][0]["migration_allowed"]
            else:
                del document["activations"][0]["workload"]["container"]
            with self.subTest(path=path):
                self.assert_validation_failure(document=document)

    def test_duplicate_json_keys_and_nonfinite_numbers_are_rejected(self):
        for raw in (
            '{"schema_version":1,"schema_version":1}',
            '{"schema_version":1,"release":{"run_id":"1","run_id":"2"}}',
            '{"schema_version":1,"value":NaN}',
        ):
            with self.subTest(raw=raw):
                self.assert_validation_failure(raw=raw)

    def test_escaped_control_characters_are_rejected(self):
        document = valid_document()
        document["release"]["repository"] = "acme/fugue\u007f"
        self.assert_validation_failure(document=document)

    def test_input_over_one_mibibyte_is_rejected_without_output(self):
        self.assert_validation_failure(raw=b" " * (1024 * 1024 + 1))

    def test_duplicate_artifact_components_are_rejected(self):
        document = valid_document()
        document["artifacts"].append(copy.deepcopy(document["artifacts"][1]))
        self.assert_validation_failure(document=document)

    def test_empty_activation_evidence_is_rejected(self):
        document = valid_document()
        document["artifacts"] = []
        document["activations"] = []
        self.assert_validation_failure(document=document)

        artifact_only = valid_document()
        artifact_only["artifacts"] = [artifact_only["artifacts"][0]]
        artifact_only["activations"] = []
        self.assert_validation_failure(document=artifact_only)

    def test_duplicate_exact_activation_binding_is_rejected(self):
        document = valid_document()
        document["activations"].append(copy.deepcopy(document["activations"][0]))
        self.assert_validation_failure(document=document)

    def test_same_workload_container_cannot_be_hidden_behind_another_binding(self):
        document = valid_document()
        first = legacy_preserve_activation()
        first["helm_path"] = "edge.image"
        duplicate = copy.deepcopy(first)
        duplicate["helm_path"] = "edge.blueGreen.slots.a.image"
        document["activations"].extend([first, duplicate])
        self.assert_validation_failure(document=document)

    def test_unknown_artifact_and_activation_components_are_rejected(self):
        artifact_document = valid_document()
        artifact_document["artifacts"][0]["component"] = "unknown"
        activation_document = valid_document()
        activation_document["activations"][0]["component"] = "app_ssh"
        for document in (artifact_document, activation_document):
            with self.subTest(component=document):
                self.assert_validation_failure(document=document)

    def test_release_identity_fields_are_strict(self):
        for field, value in (
            ("repository", "acme /fugue"),
            ("repository", "acme/fugue/extra"),
            ("workflow", "other-workflow"),
            ("run_id", "0"),
            ("run_attempt", "01"),
            ("head_sha", "A" * 40),
            ("platform", "linux/arm64"),
        ):
            document = valid_document()
            document["release"][field] = value
            with self.subTest(field=field, value=value):
                self.assert_validation_failure(document=document)

    def test_artifact_digest_ref_revision_and_verification_mismatches_fail(self):
        mutations = []
        for field, value in (
            ("top_digest", "sha256:" + "A" * 64),
            ("immutable_ref", "ghcr.io/acme/fugue-app-ssh@" + digest("9")),
            ("oci_revision", "b" * 40),
            ("source_tag", "stable"),
            ("verification", "manifest_only"),
        ):
            document = valid_document()
            document["artifacts"][0][field] = value
            mutations.append((field, document))
        for field, document in mutations:
            with self.subTest(field=field):
                self.assert_validation_failure(document=document)

    def test_built_activation_must_match_artifact_ref_and_runtime_digest(self):
        for field, value in (
            ("selected_ref", "ghcr.io/acme/fugue-api@" + digest("9")),
            ("source_template_ref", "ghcr.io/acme/fugue-api@" + digest("9")),
            ("digest", digest("9")),
            ("runtime_manifest_digest", digest("9")),
            ("source_tag", "stable"),
            ("pin_state", "legacy_unpinned"),
        ):
            document = valid_document()
            document["activations"][1][field] = value
            with self.subTest(field=field):
                self.assert_validation_failure(document=document)

    def test_legacy_unpinned_requires_explicit_migration_allowance(self):
        document = valid_document()
        document["activations"].append(legacy_preserve_activation())
        result, _, output_path = self.invoke(document=document)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(output_path.read_text())["activations"][2]["pin_state"], "legacy_unpinned")

        for field, value in (
            ("migration_allowed", False),
            ("source_mode", "migration"),
            ("digest", digest("9")),
            ("runtime_manifest_digest", digest("8")),
            ("selected_ref", "ghcr.io/acme/fugue-edge@" + digest("9")),
        ):
            invalid = valid_document()
            activation = legacy_preserve_activation()
            activation[field] = value
            invalid["activations"].append(activation)
            with self.subTest(field=field):
                self.assert_validation_failure(document=invalid)

    def test_valid_migration_pins_one_legacy_tag(self):
        document = valid_document()
        activation = legacy_preserve_activation()
        activation.update(
            {
                "source_mode": "migration",
                "selected_ref": f"{activation['repository']}@{digest('9')}",
                "digest": digest("9"),
                "runtime_manifest_digest": digest("8"),
                "pin_state": "pinned",
            }
        )
        document["activations"].append(activation)
        result, _, output_path = self.invoke(document=document)
        self.assertEqual(result.returncode, 0, result.stderr)
        output = json.loads(output_path.read_text())
        self.assertEqual(output["activations"][2]["source_mode"], "migration")

    def test_drain_agent_configuration_binding_is_allowed(self):
        document = valid_document()
        activation = pinned_preserve_activation()
        repository = "ghcr.io/acme/fugue-drain-agent"
        image_digest = activation["digest"]
        activation.update(
            {
                "component": "drain_agent",
                "source_template_ref": f"{repository}:stable@{image_digest}",
                "selected_ref": f"{repository}@{image_digest}",
                "repository": repository,
                "helm_path": "runtime.strictDrain.agent.image",
                "workload": {
                    "kind": "Configuration",
                    "name": "fugue-controller",
                    "container": "controller",
                },
            }
        )
        document["activations"].append(activation)
        result, _, _ = self.invoke(document=document)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_workload_kind_and_dns_labels_are_strict(self):
        for field, value in (
            ("kind", "StatefulSet"),
            ("name", "Uppercase"),
            ("container", "bad/name"),
        ):
            document = valid_document()
            document["activations"][0]["workload"][field] = value
            with self.subTest(field=field):
                self.assert_validation_failure(document=document)

        document = valid_document()
        document["activations"][0]["workload"]["kind"] = "Configuration"
        self.assert_validation_failure(document=document)

    def test_component_workload_kind_and_container_are_fixed(self):
        cases = []
        for component, repository, helm_path, kind, name, container in (
            (
                "api",
                "ghcr.io/acme/fugue-api",
                "api.image",
                "Deployment",
                "fugue-api-shape",
                "api",
            ),
            (
                "controller",
                "ghcr.io/acme/fugue-controller",
                "controller.image",
                "Deployment",
                "fugue-controller-shape",
                "controller",
            ),
            (
                "drain_agent",
                "ghcr.io/acme/fugue-drain-agent",
                "runtime.strictDrain.agent.image",
                "Configuration",
                "fugue-controller-drain-shape",
                "controller",
            ),
            (
                "edge",
                "ghcr.io/acme/fugue-edge",
                "edge.image",
                "DaemonSet",
                "fugue-edge-shape",
                "edge",
            ),
            (
                "image_cache",
                "ghcr.io/acme/fugue-image-cache",
                "imageCache.image",
                "DaemonSet",
                "fugue-image-cache-shape",
                "image-cache",
            ),
            (
                "telemetry_agent",
                "ghcr.io/acme/fugue-telemetry-agent",
                "observability.agent.image",
                "Deployment",
                "fugue-telemetry-agent-shape",
                "telemetry-agent",
            ),
        ):
            activation = pinned_preserve_activation()
            image_digest = activation["digest"]
            activation.update(
                {
                    "component": component,
                    "repository": repository,
                    "source_template_ref": f"{repository}:stable@{image_digest}",
                    "selected_ref": f"{repository}@{image_digest}",
                    "helm_path": helm_path,
                    "workload": {"kind": kind, "name": name, "container": container},
                }
            )
            invalid_kind = copy.deepcopy(activation)
            invalid_kind["workload"]["kind"] = (
                "DaemonSet" if kind != "DaemonSet" else "Deployment"
            )
            cases.append((component, "kind", invalid_kind))
            invalid_container = copy.deepcopy(activation)
            invalid_container["workload"]["container"] = "wrong-container"
            cases.append((component, "container", invalid_container))

        for component, field, activation in cases:
            document = valid_document()
            document["activations"].append(activation)
            with self.subTest(component=component, field=field):
                self.assert_validation_failure(document=document)

    def test_image_cache_and_telemetry_workload_shapes_are_allowed(self):
        document = valid_document()
        for component, repository, helm_path, kind, name, container in (
            (
                "image_cache",
                "ghcr.io/acme/fugue-image-cache",
                "imageCache.image",
                "DaemonSet",
                "fugue-image-cache",
                "image-cache",
            ),
            (
                "telemetry_agent",
                "ghcr.io/acme/fugue-telemetry-agent",
                "observability.agent.image",
                "Deployment",
                "fugue-telemetry-agent",
                "telemetry-agent",
            ),
        ):
            activation = pinned_preserve_activation()
            image_digest = activation["digest"]
            activation.update(
                {
                    "component": component,
                    "repository": repository,
                    "source_template_ref": f"{repository}:stable@{image_digest}",
                    "selected_ref": f"{repository}@{image_digest}",
                    "helm_path": helm_path,
                    "workload": {"kind": kind, "name": name, "container": container},
                }
            )
            document["activations"].append(activation)
        result, _, _ = self.invoke(document=document)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_controller_shared_and_materialized_consumers_are_allowed(self):
        document = valid_document()
        controller = document["activations"][0]
        for kind, name, container, helm_path in (
            ("CronJob", "fugue-registry-gc", "registry-gc", "controller.image"),
            (
                "CronJob",
                "fugue-registry-janitor",
                "registry-janitor",
                "controller.image",
            ),
            (
                "DaemonSet",
                "fugue-node-janitor",
                "node-janitor",
                "nodeJanitor.image",
            ),
            (
                "DaemonSet",
                "fugue-topology-labeler",
                "topology-labeler",
                "topologyLabeler.image",
            ),
            (
                "DaemonSet",
                "fugue-image-prepull",
                "image-prepull",
                "imagePrePull.image",
            ),
        ):
            activation = copy.deepcopy(controller)
            activation["helm_path"] = helm_path
            activation["workload"] = {
                "kind": kind,
                "name": name,
                "container": container,
            }
            document["activations"].append(activation)
        result, _, _ = self.invoke(document=document)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_safe_array_helm_paths_pass_and_injection_forms_fail(self):
        document = valid_document()
        document["activations"].append(legacy_preserve_activation())
        result, _, _ = self.invoke(document=document)
        self.assertEqual(result.returncode, 0, result.stderr)
        for helm_path in (
            "edge.groups[-1].image",
            "edge.groups[01].image",
            "edge.groups[0];touch.image",
            "edge.groups[].image",
            ".edge.image",
            "edge..image",
        ):
            invalid = valid_document()
            activation = legacy_preserve_activation()
            activation["helm_path"] = helm_path
            invalid["activations"].append(activation)
            with self.subTest(helm_path=helm_path):
                self.assert_validation_failure(document=invalid)

    def test_component_helm_paths_are_bound_to_real_chart_values(self):
        for index, helm_path in ((0, "api.image"), (1, "controller.image")):
            document = valid_document()
            document["activations"][index]["helm_path"] = helm_path
            with self.subTest(index=index, helm_path=helm_path):
                self.assert_validation_failure(document=document)

    def test_one_helm_path_cannot_select_conflicting_images(self):
        document = valid_document()
        first = legacy_preserve_activation()
        first["helm_path"] = "edge.image"
        second = copy.deepcopy(first)
        second["workload"]["name"] = "fugue-edge-other"
        second["source_template_ref"] = "ghcr.io/acme/fugue-edge:other"
        second["selected_ref"] = "ghcr.io/acme/fugue-edge:other"
        second["source_tag"] = "other"
        document["activations"].extend([first, second])
        self.assert_validation_failure(document=document)

    def test_one_helm_path_may_feed_multiple_workloads_with_one_identity(self):
        document = valid_document()
        first = legacy_preserve_activation()
        first["helm_path"] = "edge.image"
        second = copy.deepcopy(first)
        second["workload"] = {
            "kind": "DaemonSet",
            "name": "fugue-edge-other",
            "container": "edge",
        }
        document["activations"].extend([first, second])
        result, _, _ = self.invoke(document=document)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_edge_paths_are_bound_to_their_semantic_workload_shapes(self):
        valid_bindings = (
            ("edge.image", "edge"),
            ("edge.blueGreen.slots.a.image", "edge"),
            ("edge.groups[0].blueGreen.slots.b.image", "edge"),
            ("edge.blueGreen.front.image", "edge-front"),
            ("edge.dynamic.blueGreen.front.image", "edge-front"),
            ("edge.groups[1].blueGreen.front.image", "edge-front"),
            ("edge.sshFront.image", "ssh-front"),
            ("edge.groups[2].sshFront.image", "ssh-front"),
            ("dns.image", "custom-dns-container"),
            ("dns.groups[3].image", "dns"),
            ("meshRecovery.image", "mesh-recovery"),
        )
        for index, (helm_path, container) in enumerate(valid_bindings):
            document = valid_document()
            activation = legacy_preserve_activation()
            activation["helm_path"] = helm_path
            if helm_path.startswith("dns."):
                activation["workload"]["name"] = f"fugue-dns-shape-{index}"
            else:
                activation["workload"]["name"] = f"fugue-edge-shape-{index}"
            activation["workload"]["container"] = container
            document["activations"].append(activation)
            with self.subTest(helm_path=helm_path, container=container):
                result, _, _ = self.invoke(document=document)
                self.assertEqual(result.returncode, 0, result.stderr)

        for helm_path, container in (
            ("dns.image", "ssh-front"),
            ("edge.sshFront.image", "dns"),
            ("edge.blueGreen.front.image", "edge"),
            ("edge.blueGreen.slots.a.image", "edge-front"),
            ("meshRecovery.image", "edge"),
        ):
            document = valid_document()
            activation = legacy_preserve_activation()
            activation["helm_path"] = helm_path
            activation["workload"]["container"] = container
            document["activations"].append(activation)
            with self.subTest(invalid_path=helm_path, invalid_container=container):
                self.assert_validation_failure(document=document)

    def test_only_app_ssh_can_be_artifact_only_even_with_preserve_same_component(self):
        document = valid_document()
        edge_artifact = artifact("edge", "ghcr.io/acme/fugue-edge", "b")
        document["artifacts"].append(edge_artifact)
        document["activations"].append(legacy_preserve_activation())
        self.assert_validation_failure(document=document)

    def test_artifact_component_can_have_built_and_preserve_cohorts(self):
        document = valid_document()
        edge_artifact = artifact("edge", "ghcr.io/acme/fugue-edge", "b")
        document["artifacts"].append(edge_artifact)
        edge_built = built_activation(
            edge_artifact,
            helm_path="edge.groups[1].image",
            name="fugue-edge-built",
        )
        edge_built["workload"].update({"kind": "DaemonSet", "container": "edge"})
        document["activations"].extend([legacy_preserve_activation(), edge_built])
        result, _, _ = self.invoke(document=document)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
