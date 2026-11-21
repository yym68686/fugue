package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"testing"
)

func TestFrontRepairApprovedRequiresProtectedExactMainJobAndCleanExecutor(t *testing.T) {
	const repo = "fixture/fugue"
	source, declaration := strings.Repeat("a", 40), strings.Repeat("b", 40)
	temp := t.TempDir()
	inputDir := filepath.Join(temp, "inputs")
	if err := os.Mkdir(inputDir, 0700); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{
		"GITHUB_ACTIONS": "true", "GITHUB_EVENT_NAME": "push", "GITHUB_REF": "refs/heads/main", "GITHUB_REF_PROTECTED": "true",
		"GITHUB_JOB": frontRepairActionsApplyJob, "RUNNER_ENVIRONMENT": "self-hosted", "GITHUB_SERVER_URL": "https://github.com",
		"GITHUB_SHA": declaration, "GITHUB_WORKFLOW_SHA": declaration, "GITHUB_REPOSITORY": repo, "GITHUB_REPOSITORY_ID": "3",
		"GITHUB_WORKFLOW_REF": repo + "/.github/workflows/ci.yml@refs/heads/main", "RUNNER_TEMP": temp,
		"FUGUE_EDGE_FRONT_REPAIR_INPUTS": inputDir,
		frontRepairProtectedPolicyEnv:    fmt.Sprintf(`{"schema":"%s","scope":"fixture-front","component":"edge-worker-fixture","repository":"%s","repositoryId":3,"workflowId":5,"executorCommit":"%s","verifierKey":"%s"}`, frontRepairReleasePolicySchema, repo, source, strings.Repeat("A", 43)),
	}
	info := &debug.BuildInfo{Path: "fugue/cmd/fugue-release-guardian", Main: debug.Module{Path: "fugue"},
		Settings: []debug.BuildSetting{{Key: "GOOS", Value: "linux"}, {Key: "-buildmode", Value: "exe"},
			{Key: "vcs", Value: "git"}, {Key: "vcs.revision", Value: source}, {Key: "vcs.modified", Value: "false"}}}
	getenv := func(key string) string { return env[key] }
	valid, err := frontRepairActionsLaunchContext(getenv, info)
	if err != nil || valid.declaration != declaration || valid.inputs != inputDir || valid.policy.ExecutorCommit != source {
		t.Fatalf("valid protected context refused: %v", err)
	}
	for key, value := range map[string]string{
		"GITHUB_ACTIONS": "", "GITHUB_EVENT_NAME": "workflow_dispatch", "GITHUB_REF": "refs/heads/fixture",
		"GITHUB_REF_PROTECTED": "false", "GITHUB_JOB": "prepush", "RUNNER_ENVIRONMENT": "github-hosted",
		"GITHUB_WORKFLOW_SHA": "not-a-commit", "GITHUB_REPOSITORY_ID": "99", "GITHUB_WORKFLOW_REF": repo + "/other.yml@refs/heads/main",
		frontRepairProtectedPolicyEnv: "", "FUGUE_EDGE_FRONT_REPAIR_INPUTS": temp,
	} {
		t.Run(key, func(t *testing.T) {
			before := env[key]
			env[key] = value
			t.Cleanup(func() { env[key] = before })
			if _, err := frontRepairActionsLaunchContext(getenv, info); err == nil {
				t.Fatal("unprotected launch accepted")
			}
		})
	}
	info.Settings[4].Value = "true"
	if _, err := frontRepairActionsLaunchContext(getenv, info); err == nil {
		t.Fatal("dirty code accepted as an exact executor")
	}
	if err := runFrontRepairApproved([]string{"--apply"}, io.Discard); err == nil {
		t.Fatal("launcher accepted arbitrary arguments")
	}
}

func TestFrontRepairApprovedRequiresStillCurrentProtectedDeclaration(t *testing.T) {
	launch := frontRepairActionsLaunch{
		policy: frontRepairReleasePolicy{Repository: "fixture/fugue"}, declaration: strings.Repeat("a", 40)}
	expect := func(raw string, readErr error) frontRepairActionsGet {
		return func(_ context.Context, path string, limit int64) ([]byte, error) {
			if path != "repos/fixture/fugue/branches/main" || limit != frontRepairActionsMetadataMax {
				t.Fatalf("unscoped check: %s %d", path, limit)
			}
			return []byte(raw), readErr
		}
	}
	wire := fmt.Sprintf(`{"name":"main","protected":true,"commit":{"sha":"%s"}}`, launch.declaration)
	if err := requireFrontRepairCurrentDeclaration(context.Background(), launch, expect(wire, nil)); err != nil {
		t.Fatal(err)
	}
	for _, get := range []frontRepairActionsGet{
		expect(strings.Replace(wire, "true", "false", 1), nil),
		expect(strings.Replace(wire, launch.declaration, strings.Repeat("b", 40), 1), nil),
		expect(wire, errors.New("not readable")),
	} {
		if err := requireFrontRepairCurrentDeclaration(context.Background(), launch, get); err == nil {
			t.Fatal("unprotected, superseded or unknown declaration offered a launch")
		}
	}
}
