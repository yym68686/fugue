#!/usr/bin/env python3
"""Prepare a deterministic, non-production diff in a disposable clean checkout."""

from pathlib import Path
import argparse
import subprocess


ROOT = Path(__file__).resolve().parent.parent

QUEUE_RUNTIME = (
    "internal/operationqueue/selection.go",
    "\tif (!model.OperationWaitingForImageReplication(op) && !model.OperationWaitingForTrafficConfirmation(op) && !model.OperationWaitingForCanonicalHandoff(op)) || op.UpdatedAt.IsZero() {\n",
    "\tif op.UpdatedAt.IsZero() {\n"
    "\t\treturn true\n"
    "\t}\n"
    "\tif !model.OperationWaitingForImageReplication(op) && !model.OperationWaitingForTrafficConfirmation(op) && !model.OperationWaitingForCanonicalHandoff(op) {\n",
)

QUEUE_TEST = (
    "internal/operationqueue/queue_test.go",
    "func TestAppCPUResizeRestoresUseDedicatedWorkerLane(t *testing.T) {\n",
    "func TestRestoreLaneDoesNotAcceptOrdinaryPendingDeploy(t *testing.T) {\n"
    "\tif pendingOperationMatchesLane(pending(\"deploy\", \"app\", model.OperationTypeDeploy), CPURequestRestore) {\n"
    "\t\tt.Fatal(\"ordinary deployments must not occupy the restore lane\")\n"
    "\t}\n"
    "}\n\n"
    "func TestAppCPUResizeRestoresUseDedicatedWorkerLane(t *testing.T) {\n",
)

HOST_POLICY_TEST = (
    "internal/nodehostpolicy/host_memory_safety_test.go",
    "func TestHostMemorySafetyShellHasValidBashSyntax(t *testing.T) {\n",
    "func TestHostMemorySafetyShellHasValidBashSyntax(t *testing.T) {\n"
    "\t// The rendered fragment must be valid on its own, before an installer runs it.\n",
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

CASES = {
    "queue-runtime": (QUEUE_RUNTIME,),
    "queue-test": (QUEUE_TEST,),
    "cross-domain": (QUEUE_RUNTIME, HOST_POLICY_TEST),
}

CONTRACTS = {
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
