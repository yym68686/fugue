#!/usr/bin/env python3

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import textwrap
import unittest


ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "build_control_plane_images.sh"
WORKFLOW = ROOT / ".github" / "workflows" / "deploy-control-plane.yml"


def digest(character: str) -> str:
    return "sha256:" + character * 64


class ControlPlaneBuildReuseTest(unittest.TestCase):
    revision = "1" * 40
    repository = "registry.test/acme/fugue-api"

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.runner_temp = self.directory / "runner"
        self.runner_temp.mkdir(mode=0o700)
        self.bin_dir = self.directory / "bin"
        self.bin_dir.mkdir(mode=0o700)
        self.verify_calls = self.directory / "verify-calls.jsonl"
        self.build_calls = self.directory / "build-calls"
        self.output = self.directory / "github-output"
        self.output.write_bytes(b"")
        os.chmod(self.output, 0o600)

        self.verifier = self.directory / "verify_registry_fixture.py"
        self.verifier.write_text(
            textwrap.dedent(
                """\
                import argparse
                import json
                import os
                from pathlib import Path

                parser = argparse.ArgumentParser()
                parser.add_argument("--image", required=True)
                parser.add_argument("--platform", required=True)
                parser.add_argument("--expected-revision", required=True)
                parser.add_argument("--timeout-seconds")
                parser.add_argument("--request-timeout-seconds")
                parser.add_argument("--max-attempts")
                parser.add_argument("--retry-delay-seconds")
                parser.add_argument("--metadata-only", action="store_true")
                arguments = parser.parse_args()
                with Path(os.environ["VERIFY_CALLS"]).open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps(vars(arguments), separators=(",", ":"), sort_keys=True) + "\\n")
                top_digest = arguments.image.rsplit("@", 1)[1]
                config_digest = os.environ["EXPECTED_CONFIG_DIGEST"]
                if os.environ.get("VERIFY_MISMATCH") == "config":
                    config_digest = "sha256:" + "9" * 64
                value = {
                    "blob_count": 3,
                    "config_digest": config_digest,
                    "image": arguments.image,
                    "index_digest": top_digest,
                    "layer_get_probe_count": 0 if arguments.metadata_only else 2,
                    "manifest_digest": os.environ["EXPECTED_PLATFORM_DIGEST"],
                    "oci_revision": arguments.expected_revision,
                    "platform": arguments.platform,
                    "request_count": 7,
                    "total_layer_bytes": 1234,
                    "verification": (
                        "registry_manifest_config_get"
                        if arguments.metadata_only
                        else "registry_manifest_config_and_layer_get"
                    ),
                }
                print(json.dumps(value, separators=(",", ":"), sort_keys=True))
                """
            ),
            encoding="utf-8",
        )
        os.chmod(self.verifier, 0o600)
        docker = self.bin_dir / "docker"
        docker.write_text(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >>\"${BUILD_CALLS}\"\nexit 97\n",
            encoding="utf-8",
        )
        os.chmod(docker, 0o700)

    def artifact(self, revision: str | None = None) -> dict[str, str]:
        revision = revision or self.revision
        top_digest = digest("a")
        return {
            "component": "api",
            "config_digest": digest("b"),
            "immutable_ref": f"{self.repository}@{top_digest}",
            "oci_revision": revision,
            "platform_manifest_digest": digest("c"),
            "repository": self.repository,
            "source_tag": revision,
            "top_digest": top_digest,
            "verification": "registry_manifest_config_and_layer_get",
        }

    def write_receipt(self, artifacts: list[dict[str, str]]) -> Path:
        path = self.directory / f"receipt-{len(list(self.directory.glob('receipt-*')))}.json"
        path.write_bytes(json.dumps(artifacts, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode("ascii"))
        os.chmod(path, 0o600)
        return path

    def invoke(
        self,
        receipt: Path,
        *,
        targets: str = "api",
        extra_environment: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        environment = os.environ.copy()
        environment.update(
            {
                "PATH": f"{self.bin_dir}:{environment['PATH']}",
                "RUNNER_TEMP": str(self.runner_temp),
                "GITHUB_OUTPUT": str(self.output),
                "FUGUE_CONTROL_PLANE_IMAGE_TARGETS": targets,
                "FUGUE_IMAGE_TAG": self.revision,
                "FUGUE_API_IMAGE_REPOSITORY": self.repository,
                "FUGUE_CONTROL_PLANE_BUILD_RECEIPT_FILE": str(receipt),
                "FUGUE_REGISTRY_IMAGE_VERIFIER": str(self.verifier),
                "EXPECTED_CONFIG_DIGEST": digest("b"),
                "EXPECTED_PLATFORM_DIGEST": digest("c"),
                "VERIFY_CALLS": str(self.verify_calls),
                "BUILD_CALLS": str(self.build_calls),
            }
        )
        if extra_environment:
            environment.update(extra_environment)
        return subprocess.run(
            ["bash", str(SCRIPT)],
            cwd=ROOT,
            env=environment,
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )

    def assert_zero_build(self) -> None:
        self.assertFalse(self.build_calls.exists() and self.build_calls.read_text(encoding="utf-8"))

    def assert_no_outputs(self) -> None:
        self.assertEqual(self.output.read_bytes(), b"")

    def test_receipt_hit_freshly_verifies_and_publishes_build_equivalent_outputs(self) -> None:
        artifact = self.artifact()
        receipt = self.write_receipt([artifact])
        result = self.invoke(receipt)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assert_zero_build()
        calls = [json.loads(line) for line in self.verify_calls.read_text(encoding="utf-8").splitlines()]
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["image"], artifact["immutable_ref"])
        self.assertEqual(calls[0]["expected_revision"], self.revision)
        self.assertTrue(calls[0]["metadata_only"])
        self.assertEqual(calls[0]["timeout_seconds"], "18")
        receipt_bytes = receipt.read_bytes()
        expected = (
            f"api_image_digest={artifact['top_digest']}\n"
            f"verified_image_artifacts_json={receipt_bytes.decode('ascii')}\n"
            f"verified_image_artifacts_digest=sha256:{hashlib.sha256(receipt_bytes).hexdigest()}\n"
        )
        self.assertEqual(self.output.read_text(encoding="utf-8"), expected)

    def test_registry_mismatch_fails_after_fresh_get_with_zero_build(self) -> None:
        result = self.invoke(self.write_receipt([self.artifact()]), extra_environment={"VERIFY_MISMATCH": "config"})
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("registry-reverification-receipt-mismatch", result.stderr)
        self.assertEqual(len(self.verify_calls.read_text(encoding="utf-8").splitlines()), 1)
        self.assert_zero_build()
        self.assert_no_outputs()

    def test_stale_receipt_fails_before_registry_with_zero_build(self) -> None:
        result = self.invoke(self.write_receipt([self.artifact("2" * 40)]))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("build-receipt-stale:api", result.stderr)
        self.assertFalse(self.verify_calls.exists())
        self.assert_zero_build()
        self.assert_no_outputs()

    def test_missing_component_fails_before_registry_with_zero_build(self) -> None:
        result = self.invoke(
            self.write_receipt([self.artifact()]),
            targets="api controller",
            extra_environment={"FUGUE_CONTROLLER_IMAGE_REPOSITORY": "registry.test/acme/fugue-controller"},
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("build-receipt-artifact-set-missing", result.stderr)
        self.assertFalse(self.verify_calls.exists())
        self.assert_zero_build()
        self.assert_no_outputs()

    def test_noncanonical_receipt_fails_before_registry_with_zero_build(self) -> None:
        receipt = self.write_receipt([self.artifact()])
        receipt.write_bytes(receipt.read_bytes() + b"\n")
        result = self.invoke(receipt)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("build-receipt-canonical-bytes-invalid", result.stderr)
        self.assertFalse(self.verify_calls.exists())
        self.assert_zero_build()
        self.assert_no_outputs()

    def test_workflow_reuse_is_bounded_to_exact_api_hotfix(self) -> None:
        source = WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("[[ -z \"${ARTIFACT_REUSE_RECEIPT_B64}\" ]]", source)
        self.assertIn(
            "[[ -n \"${ARTIFACT_REUSE_RECEIPT_B64}\" && ${#ARTIFACT_REUSE_RECEIPT_B64} -le 196608 ]]",
            source,
        )
        self.assertIn("CONFIRM_CONTROL_PLANE_API_HOTFIX_ROLLOUT_V2_57DC", source)
        self.assertIn("57dc767999741cea25fe4820a6c9603984dfa0b9", source)

    def test_workflow_skips_every_build_setup_on_receipt_hit(self) -> None:
        source = WORKFLOW.read_text(encoding="utf-8")
        receipt_empty = "inputs.artifact_reuse_receipt_b64 == ''"
        for name in ("Setup Go", "Setup Docker Buildx", "Login to GHCR"):
            marker = f"      - name: {name}\n"
            start = source.index(marker)
            end = source.find("\n      - name:", start + len(marker))
            step = source[start : end if end >= 0 else len(source)]
            self.assertIn(receipt_empty, step, name)
        self.assertIn("timeout --signal=TERM --kill-after=1s 19s ./current-release-tools/scripts/build_control_plane_images.sh", source)
        self.assertIn("FUGUE_CONTROL_PLANE_BUILD_RECEIPT_FILE:", source)


if __name__ == "__main__":
    unittest.main()
