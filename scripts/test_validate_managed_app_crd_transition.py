#!/usr/bin/env python3

import copy
import importlib.util
import json
import os
import pathlib
import subprocess
import tempfile
import unittest


MODULE_PATH = pathlib.Path(__file__).with_name("validate_managed_app_crd_transition.py")
SPEC = importlib.util.spec_from_file_location("managed_app_crd_transition", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def scalar(kind):
    return {"type": kind}


def backing_services_schema():
    return {
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                field: scalar(kind)
                for field, kind in MODULE.REQUIRED_BACKING_SERVICE_FIELDS.items()
            },
        },
    }


def document(status_properties):
    return {
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": MODULE.CRD_NAME, "resourceVersion": "12345"},
        "spec": {
            "group": "fugue.pro",
            "names": {
                "kind": "ManagedApp",
                "plural": "managedapps",
                "singular": "managedapp",
            },
            "scope": "Namespaced",
            "versions": [
                {
                    "name": "v1alpha1",
                    "served": True,
                    "storage": True,
                    "schema": {
                        "openAPIV3Schema": {
                            "type": "object",
                            "properties": {
                                "spec": {"type": "object"},
                                "status": {
                                    "type": "object",
                                    "properties": copy.deepcopy(status_properties),
                                },
                            },
                        }
                    },
                    "subresources": {"status": {}},
                }
            ],
        },
    }


def current_properties():
    return {
        "phase": scalar("string"),
        "message": scalar("string"),
        "readyReplicas": scalar("integer"),
        "desiredReplicas": scalar("integer"),
    }


def target_properties():
    properties = current_properties()
    properties["backingServices"] = backing_services_schema()
    properties.update(
        {field: scalar(kind) for field, kind in MODULE.REQUIRED_RELEASE_FIELDS.items()}
    )
    return properties


class ManagedAppCRDTransitionTest(unittest.TestCase):
    def test_accepts_only_additive_status_properties(self):
        additions = MODULE.validate_transition(
            document(current_properties()), document(target_properties())
        )
        self.assertEqual(
            additions,
            [
                "v1alpha1.backingServices",
                "v1alpha1.currentReleaseKey",
                "v1alpha1.currentReleaseReadyAt",
                "v1alpha1.currentReleaseStartedAt",
                "v1alpha1.pendingReleaseKey",
                "v1alpha1.pendingReleaseStartedAt",
            ],
        )

    def test_accepts_converged_schema_as_noop(self):
        target = document(target_properties())
        self.assertEqual(MODULE.validate_transition(target, copy.deepcopy(target)), [])

    def test_rejects_existing_status_property_change(self):
        target = document(target_properties())
        target["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"][
            "status"
        ]["properties"]["phase"] = scalar("integer")
        with self.assertRaisesRegex(MODULE.TransitionError, "changes existing status property"):
            MODULE.validate_transition(document(current_properties()), target)

    def test_rejects_spec_change_outside_status_properties(self):
        target = document(target_properties())
        target["spec"]["scope"] = "Cluster"
        with self.assertRaisesRegex(MODULE.TransitionError, "outside additive status properties"):
            MODULE.validate_transition(document(current_properties()), target)

    def test_rejects_missing_required_backing_service_shape(self):
        properties = target_properties()
        del properties["backingServices"]["items"]["properties"]["serviceID"]
        with self.assertRaisesRegex(MODULE.TransitionError, "serviceID"):
            MODULE.validate_transition(document(current_properties()), document(properties))

    def test_rejects_status_property_removal(self):
        live = document(target_properties())
        target = document(target_properties())
        del target["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"][
            "status"
        ]["properties"]["message"]
        with self.assertRaisesRegex(MODULE.TransitionError, "removes existing status property"):
            MODULE.validate_transition(live, target)

    def test_json_patch_is_resource_version_guarded_and_additive_only(self):
        live = document(current_properties())
        target = document(target_properties())
        patch = MODULE.build_json_patch(live, target)
        self.assertEqual(
            patch[:2],
            [
                {"op": "test", "path": "/metadata/name", "value": MODULE.CRD_NAME},
                {
                    "op": "test",
                    "path": "/metadata/resourceVersion",
                    "value": "12345",
                },
            ],
        )
        self.assertEqual(len(patch), 8)
        self.assertTrue(all(operation["op"] == "add" for operation in patch[2:]))
        self.assertEqual(
            [operation["path"].rsplit("/", 1)[-1] for operation in patch[2:]],
            [
                addition.split(".", 1)[1]
                for addition in MODULE.validate_transition(live, target)
            ],
        )

    def test_json_patch_requires_resource_version(self):
        live = document(current_properties())
        del live["metadata"]["resourceVersion"]
        with self.assertRaisesRegex(MODULE.TransitionError, "resourceVersion"):
            MODULE.build_json_patch(live, document(target_properties()))


class ManagedAppCRDSyncScriptTest(unittest.TestCase):
    def run_sync(self, live, target):
        root = pathlib.Path(__file__).resolve().parent.parent
        with tempfile.TemporaryDirectory() as temporary:
            temporary = pathlib.Path(temporary)
            live_path = temporary / "live.json"
            target_path = temporary / "target.json"
            state_path = temporary / "state"
            calls_path = temporary / "calls"
            fake_kubectl = temporary / "kubectl"
            fake_timeout = temporary / "timeout"
            live_path.write_text(json.dumps(live), encoding="utf-8")
            target_path.write_text(json.dumps(target), encoding="utf-8")
            fake_kubectl.write_text(
                """#!/usr/bin/env bash
set -euo pipefail
printf '%s\\n' "$*" >>"${CALLS_FILE}"
case "${1:-}" in
  get)
    if [[ -f "${STATE_FILE}" ]]; then cat "${TARGET_FILE}"; else cat "${LIVE_FILE}"; fi
    ;;
  apply)
    if [[ " $* " == *' --dry-run=server '* ]]; then
      cat "${TARGET_FILE}"
    else
      exit 2
    fi
    ;;
  patch)
    if [[ " $* " == *' --dry-run=server '* ]]; then
      cat "${TARGET_FILE}"
    else
      printf 'applied\\n' >"${STATE_FILE}"
      printf 'customresourcedefinition.apiextensions.k8s.io/managedapps.fugue.pro configured\\n'
    fi
    ;;
  wait) exit 0 ;;
  diff)
    if [[ -f "${STATE_FILE}" ]] || cmp -s "${LIVE_FILE}" "${TARGET_FILE}"; then exit 0; fi
    exit 1
    ;;
  *) exit 2 ;;
esac
""",
                encoding="utf-8",
            )
            fake_kubectl.chmod(0o700)
            fake_timeout.write_text(
                """#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == --kill-after=* ]]; then shift; fi
[[ "${1:-}" =~ ^[0-9]+s$ ]] || exit 2
shift
exec "$@"
""",
                encoding="utf-8",
            )
            fake_timeout.chmod(0o700)
            env = os.environ.copy()
            env.update(
                {
                    "CALLS_FILE": str(calls_path),
                    "KUBECTL_BIN": str(fake_kubectl),
                    "LIVE_FILE": str(live_path),
                    "PATH": str(temporary) + os.pathsep + env.get("PATH", ""),
                    "STATE_FILE": str(state_path),
                    "TARGET_FILE": str(target_path),
                }
            )
            result = subprocess.run(
                ["bash", str(root / "scripts" / "sync_managed_app_crd.sh")],
                cwd=root,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                check=False,
            )
            calls = calls_path.read_text(encoding="utf-8") if calls_path.exists() else ""
            return result, calls, state_path.exists()

    def test_sync_script_applies_and_verifies_additive_schema(self):
        result, calls, applied = self.run_sync(
            document(current_properties()), document(target_properties())
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertTrue(applied)
        self.assertIn("patch crd/managedapps.fugue.pro --type=json --patch-file", calls)
        self.assertIn("--dry-run=server", calls)
        self.assertIn("wait --for=condition=Established", calls)
        self.assertIn("diff -f", calls)
        self.assertIn("schema synchronization verified", result.stdout)

    def test_sync_script_rejects_non_additive_schema_before_apply(self):
        target = document(target_properties())
        target["spec"]["scope"] = "Cluster"
        result, calls, applied = self.run_sync(document(current_properties()), target)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertFalse(applied)
        self.assertNotIn("wait --for=condition=Established", calls)
        self.assertNotIn("diff -f", calls)


if __name__ == "__main__":
    unittest.main()
