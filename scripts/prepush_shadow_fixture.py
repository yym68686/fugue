#!/usr/bin/env python3
"""Prepare a deterministic, non-production diff in a disposable clean checkout."""

from pathlib import Path
import argparse
import subprocess


ROOT = Path(__file__).resolve().parent.parent
SAMPLE_MARKER = "fugue_shadow_sample_001"

QUEUE_RUNTIME = (
    "internal/operationqueue/selection.go",
    '"list pending deploy apps: %w"',
    f'"list pending deploy apps ({SAMPLE_MARKER}): %w"',
)

QUEUE_TEST = (
    "internal/operationqueue/queue_test.go",
    "func TestAppCPUResizeRestoresUseDedicatedWorkerLane(t *testing.T) {\n",
    f"func TestRestoreLaneDoesNotAcceptOrdinaryPendingDeploy_{SAMPLE_MARKER}(t *testing.T) {{\n"
    "\tif pendingOperationMatchesLane(pending(\"deploy\", \"app\", model.OperationTypeDeploy), CPURequestRestore) {\n"
    "\t\tt.Fatal(\"ordinary deployments must not occupy the restore lane\")\n"
    "\t}\n"
    "}\n\n"
    "func TestAppCPUResizeRestoresUseDedicatedWorkerLane(t *testing.T) {\n",
)

HOST_POLICY_TEST = (
    "internal/nodehostpolicy/host_memory_safety_test.go",
    "func TestHostMemorySafetyShellHasValidBashSyntax(t *testing.T) {\n",
    f"func TestHostMemorySafetyShellHasValidBashSyntax_{SAMPLE_MARKER}(t *testing.T) {{\n",
)

LOCALPV_PYTHON_TEST = (
    "internal/localpvsafety/host/test_localpv_unit.py",
    "    def test_observe_reports_drift_without_writes(self):\n",
    f"    def test_observe_reports_drift_without_writes_{SAMPLE_MARKER}(self):\n",
)

BACKUP_RUNTIME = (
    "internal/backupusage/inventoryscan/service.go",
    '"backup namespace is not measurable"',
    f'"backup namespace is not measurable ({SAMPLE_MARKER})"',
)

RETIREMENT_TEST = (
    "internal/releaseretirement/scheduler_test.go",
    "func TestSweepOnlyBacksOffAttemptsWithoutProgress(t *testing.T) {\n",
    f"func TestSweepOnlyBacksOffAttemptsWithoutProgress_{SAMPLE_MARKER}(t *testing.T) {{\n",
)

BUILDER_RUNTIME = (
    "internal/buildadmission/failure.go",
    '"unsupported permanent build failure ledger"',
    f'"unsupported permanent build failure ledger ({SAMPLE_MARKER})"',
)

BUILDER_TEST = (
    "internal/buildadmission/placement_test.go",
    "func TestEvaluatePlacementRequiresEligibleAllocationAbovePreviousOOM(t *testing.T) {\n",
    f"func TestEvaluatePlacementRequiresEligibleAllocationAbovePreviousOOM_{SAMPLE_MARKER}(t *testing.T) {{\n",
)

OOM_RUNTIME = (
    "internal/oomrightsizing/sweep.go",
    '"OOM right-sizing for app=%s failed: %v"',
    f'"OOM right-sizing ({SAMPLE_MARKER}) for app=%s failed: %v"',
)

OOM_TEST = (
    "internal/oomrightsizing/sweep_test.go",
    "func TestLocalClaimExpiresWithoutExposingState(t *testing.T) {\n",
    f"func TestLocalClaimExpiresWithoutExposingState_{SAMPLE_MARKER}(t *testing.T) {{\n",
)

TRACKING_SCOPE_REGRESSION = (
    "internal/controller/candidate_startup_budget.go",
    "s.Store.ReadImageTracking().GetAppImageTracking(",
    "s.Store.ImageTracking().GetAppImageTracking(",
)

GENERATOR_REGRESSION = (
    "cmd/fugue-openapi-gen/main.go",
    'b.WriteString("package api\\n")',
    'b.WriteString("package api\\n// Generated source differs from the checked-in routes.\\n")',
)

RETIREMENT_SCOPE_REGRESSION = (
    "internal/controller/safe_rollout_resume.go",
    "\treleases, err := s.Store.ListAppReleases(",
    "\t_ = s.Store.AppReleaseRetirement\n\treleases, err := s.Store.ListAppReleases(",
)

BACKUP_SCOPE_REGRESSION = (
    "internal/api/server.go",
    "\tbackupInventory := newBackupInventory(store, cfg.BackupInventory)\n",
    "\t_ = store.BackupInventoryCheckpointWriter\n"
    "\tbackupInventory := newBackupInventory(store, cfg.BackupInventory)\n",
)

BUILDER_SCOPE_REGRESSION = (
    "internal/controller/safe_rollout_resume.go",
    "\treleases, err := s.Store.ListAppReleases(",
    "\t_ = s.Store.BuilderAdmissionCheckpoints\n\treleases, err := s.Store.ListAppReleases(",
)

OOM_SCOPE_REGRESSION = (
    "internal/controller/safe_rollout_resume.go",
    "\treleases, err := s.Store.ListAppReleases(",
    "\t_ = s.Store.EnqueueOOMRightSizingDeploy\n\treleases, err := s.Store.ListAppReleases(",
)

LOCALPV_INVENTORY_SCOPE_REGRESSION = (
    "internal/controller/safe_rollout_resume.go",
    "\treleases, err := s.Store.ListAppReleases(",
    "\t_ = s.Store.LocalPVInventoryReporter\n\treleases, err := s.Store.ListAppReleases(",
)

CASES = {
    "queue-runtime": (QUEUE_RUNTIME,),
    "queue-test": (QUEUE_TEST,),
    "host-policy-test": (HOST_POLICY_TEST,),
    "localpv-python-test": (LOCALPV_PYTHON_TEST,),
    "backup-runtime": (BACKUP_RUNTIME,),
    "retirement-test": (RETIREMENT_TEST,),
    "builder-runtime": (BUILDER_RUNTIME,),
    "builder-test": (BUILDER_TEST,),
    "oom-runtime": (OOM_RUNTIME,),
    "oom-test": (OOM_TEST,),
    "cross-domain": (QUEUE_RUNTIME, HOST_POLICY_TEST),
}

CONTRACTS = {
    "contract-localpv-inventory": (
        LOCALPV_INVENTORY_SCOPE_REGRESSION, "./internal/localpvsafety/inventory",
        "TestLocalPVInventoryWritesStayOnTheirDomainPorts",
        "only internal/api/image_cache_localpv_admin.go may acquire LocalPVInventoryReporter",
    ),
    "contract-builder-checkpoint": (
        BUILDER_SCOPE_REGRESSION, "./internal/buildadmission",
        "TestOnlyControllerAdapterAcquiresBuilderAdmissionCheckpoints",
        "acquire builder admission checkpoints only in the Controller adapter",
    ),
    "contract-oom-admission": (
        OOM_SCOPE_REGRESSION, "./internal/oomrightsizing",
        "TestOnlyAPIAdapterUsesOOMRightSizingWritePort",
        "only the API OOM adapter may acquire this write port",
    ),
    "contract-backup-checkpoint": (
        BACKUP_SCOPE_REGRESSION, "./internal/backupusage",
        "TestOnlyBackupInventoryAssemblyAcquiresCheckpointWriter",
        "acquire backup inventory checkpoint writer only in its assembly",
    ),
    "contract-tracking-scope": (
        TRACKING_SCOPE_REGRESSION, "./internal/imagetracking",
        "TestTrackingWriteCapabilityIsWiredOnlyAtComposition",
        "acquire image tracking write capability only in its composition adapter",
    ),
    "contract-generator-consumer": (
        GENERATOR_REGRESSION, "./internal/apispec",
        "TestGeneratedArtifactsAreUpToDate",
        "generated file is out of date:",
    ),
    "contract-retirement-scope": (
        RETIREMENT_SCOPE_REGRESSION, "./internal/releaseretirement",
        "TestOnlyControllerCompositionAcquiresReleaseRetirementWritePort",
        "acquire the retirement write port only in its Controller composition adapter",
    ),
}


def prepare(case, root=ROOT):
    root = Path(root)
    status = subprocess.check_output(["git", "status", "--porcelain"], cwd=root)
    if status:
        raise ValueError("fixture requires a clean disposable checkout")
    pending = []
    edits = (CONTRACTS[case][0],) if case in CONTRACTS else CASES[case]
    for name, before, after in edits:
        path = root / name
        source = path.read_text(encoding="utf-8")
        if source.count(before) != 1:
            raise ValueError(f"fixture marker changed: {name}")
        pending.append((path, source.replace(before, after, 1)))
    for path, source in pending:
        path.write_text(source, encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", choices=CASES, required=True)
    args = parser.parse_args()
    try:
        prepare(args.case)
    except (OSError, ValueError, subprocess.CalledProcessError) as exc:
        parser.exit(2, f"fixture unavailable: {exc}\n")


if __name__ == "__main__":
    main()
