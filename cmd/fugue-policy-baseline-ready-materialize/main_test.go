package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMaterializePolicyBaselineReady(t *testing.T) {
	t.Parallel()
	checkpoint, environment, timing := validDocuments()
	stdout, stderr, status := runFixture(t, checkpoint, environment, timing, nil)
	if status != 0 || stderr != "" {
		t.Fatalf("run status=%d stderr=%q", status, stderr)
	}
	if !canonicalOneLF(stdout) || bytes.Count(stdout, []byte{'\n'}) != 1 {
		t.Fatalf("output is not canonical one-LF JSON: %q", stdout)
	}
	second, secondStderr, secondStatus := runFixture(t, checkpoint, environment, timing, nil)
	if secondStatus != 0 || secondStderr != "" || !bytes.Equal(stdout, second) {
		t.Fatalf("materialization is nondeterministic: status=%d stderr=%q", secondStatus, secondStderr)
	}
	var document baselineReadyDocument
	if err := json.Unmarshal(stdout, &document); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if document.Status != "BASELINE_READY" || document.BaseSHA != strings.Repeat("a", 40) || document.BaseTree != strings.Repeat("b", 40) ||
		document.Source.SafetyStatus != "PASS" || document.Source.TimingStatus != "TIMING_NONCOMPLIANT" ||
		document.DiskAvailable != 12*1024*1024 || document.LoopbackPort != 63006 {
		t.Fatalf("unexpected document: %+v", document)
	}
}

func TestMaterializePolicyBaselineReadyFailsClosed(t *testing.T) {
	t.Parallel()
	tests := map[string]func(map[string]any, map[string]any, map[string]any, *[]string){
		"checkpoint identity drift": func(c, _, _ map[string]any, _ *[]string) {
			c["commit"].(map[string]any)["sha"] = strings.Repeat("c", 40)
		},
		"checkpoint incomplete": func(c, _, _ map[string]any, _ *[]string) { c["status"] = "commit_created" },
		"full gate failed":      func(c, _, _ map[string]any, _ *[]string) { c["full_gate"].(map[string]any)["result"] = "FAIL" },
		"review open":           func(c, _, _ map[string]any, _ *[]string) { c["independent_review"].(map[string]any)["ended"] = false },
		"planner runtime domain": func(c, _, _ map[string]any, _ *[]string) {
			c["planner_gate"].(map[string]any)["domains"] = []any{"controller"}
		},
		"planner domains omitted": func(c, _, _ map[string]any, _ *[]string) {
			delete(c["planner_gate"].(map[string]any), "domains")
		},
		"planner domains null": func(c, _, _ map[string]any, _ *[]string) {
			c["planner_gate"].(map[string]any)["domains"] = nil
		},
		"production write": func(c, _, _ map[string]any, _ *[]string) {
			c["publication"].(map[string]any)["production_write"] = true
		},
		"production write omitted": func(c, _, _ map[string]any, _ *[]string) {
			delete(c["publication"].(map[string]any), "production_write")
		},
		"production write null": func(c, _, _ map[string]any, _ *[]string) {
			c["publication"].(map[string]any)["production_write"] = nil
		},
		"observation too short": func(c, _, _ map[string]any, _ *[]string) { c["observation"].(map[string]any)["samples"] = float64(4) },
		"recovery failed":       func(c, _, _ map[string]any, _ *[]string) { c["recovery_proof"].(map[string]any)["result"] = "FAIL" },
		"disk below floor":      func(_, e, _ map[string]any, _ *[]string) { e["disk_available_kib"] = float64(10485759) },
		"residual process":      func(_, e, _ map[string]any, _ *[]string) { e["residual_processes"] = []any{"go test ./..."} },
		"residual processes omitted": func(_, e, _ map[string]any, _ *[]string) {
			delete(e, "residual_processes")
		},
		"residual processes null": func(_, e, _ map[string]any, _ *[]string) { e["residual_processes"] = nil },
		"remote mutation omitted": func(c, _, _ map[string]any, _ *[]string) {
			delete(c["recovery_proof"].(map[string]any), "remote_ref_mutation")
		},
		"remote mutation null": func(c, _, _ map[string]any, _ *[]string) {
			c["recovery_proof"].(map[string]any)["remote_ref_mutation"] = nil
		},
		"production mutation omitted": func(c, _, _ map[string]any, _ *[]string) {
			delete(c["recovery_proof"].(map[string]any), "production_mutation")
		},
		"production mutation null": func(c, _, _ map[string]any, _ *[]string) {
			c["recovery_proof"].(map[string]any)["production_mutation"] = nil
		},
		"timing safety failed": func(_, _, tm map[string]any, _ *[]string) { tm["safety_status"] = "FAIL" },
		"unsupported timing":   func(_, _, tm map[string]any, _ *[]string) { tm["timing_status"] = "unknown" },
		"attestation mismatch": func(c, _, _ map[string]any, _ *[]string) { c["__corrupt_checkpoint_digest"] = true },
		"symlink input":        func(c, _, _ map[string]any, _ *[]string) { c["__symlink_environment"] = true },
	}
	for name, mutate := range tests {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			checkpoint, environment, timing := validDocuments()
			stdout, stderr, status := runFixture(t, checkpoint, environment, timing, mutate)
			if status != 1 || string(stdout) != "" || stderr != invalidEvidenceMessage {
				t.Fatalf("status=%d stdout=%q stderr=%q", status, stdout, stderr)
			}
		})
	}
}

func TestMaterializePolicyBaselineReadyRejectsInvalidInvocation(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	if status := run(nil, &stdout, &stderr); status != 1 || stdout.Len() != 0 || stderr.String() != invalidInvocationMessage {
		t.Fatalf("status=%d stdout=%q stderr=%q", status, stdout.String(), stderr.String())
	}
}

func validDocuments() (map[string]any, map[string]any, map[string]any) {
	sha, tree := strings.Repeat("a", 40), strings.Repeat("b", 40)
	checkpoint := map[string]any{
		"checkpoint": "RP2P-test", "status": "checkpoint_complete",
		"commit":             map[string]any{"sha": sha, "tree": tree},
		"full_gate":          map[string]any{"result": "PASS", "candidate_tree": tree},
		"independent_review": map[string]any{"result": "APPROVE", "ended": true},
		"planner_gate":       map[string]any{"result": "unknown", "domains": []any{}, "exit_code": float64(2), "zero_runtime_domains": true},
		"publication":        map[string]any{"main_sha": sha, "ci_result": "success", "build_result": "success", "formal_result": "success", "production_write": false},
		"observation":        map[string]any{"result": "PASS", "samples": float64(5), "main_stable": true, "actions_and_artifacts_stable": true, "api_health": "ok", "central_coredns": "5/5 1/1 1"},
		"recovery_proof":     map[string]any{"result": "PASS", "remote_ref_mutation": false, "production_mutation": false},
	}
	environment := map[string]any{
		"schema_version": float64(1), "status": "ENVIRONMENT_READY", "qualified_at_utc": "2026-07-19T15:10:57Z",
		"environment_class": "macos-hosted-unrestricted-loopback", "disk_available_kib": float64(12 * 1024 * 1024),
		"disk_minimum_kib": float64(10 * 1024 * 1024), "loopback_shared_udp_tcp_port": float64(63006), "residual_processes": []any{},
		"toolchain":            map[string]any{"go": "go1.25.7 darwin/arm64", "git": "2.50.1 Apple Git-155", "helm": "v4.2.0", "gh": "2.57.0"},
		"canonical_user_files": map[string]any{"result": "PASS"},
	}
	timing := map[string]any{"checkpoint": "RP2P-test", "candidate_sha": sha, "candidate_tree": tree, "safety_status": "PASS", "timing_status": "TIMING_NONCOMPLIANT"}
	return checkpoint, environment, timing
}

func runFixture(t *testing.T, checkpoint, environment, timing map[string]any, mutate func(map[string]any, map[string]any, map[string]any, *[]string)) ([]byte, string, int) {
	t.Helper()
	root := t.TempDir()
	paths := []string{filepath.Join(root, "checkpoint.json"), filepath.Join(root, "environment.json"), filepath.Join(root, "timing.json")}
	documents := []map[string]any{checkpoint, environment, timing}
	digests := make([]string, len(documents))
	for index, document := range documents {
		data, err := json.Marshal(document)
		if err != nil {
			t.Fatalf("marshal fixture: %v", err)
		}
		if err := os.WriteFile(paths[index], data, 0o600); err != nil {
			t.Fatalf("write fixture: %v", err)
		}
		digest := sha256.Sum256(data)
		digests[index] = "sha256:" + hex.EncodeToString(digest[:])
	}
	args := []string{
		"--checkpoint", paths[0], "--environment", paths[1], "--timing", paths[2],
		"--checkpoint-digest", digests[0], "--environment-digest", digests[1], "--timing-digest", digests[2],
		"--expected-sha", strings.Repeat("a", 40), "--expected-tree", strings.Repeat("b", 40),
	}
	if mutate != nil {
		mutate(checkpoint, environment, timing, &args)
		for index, document := range documents {
			data, err := json.Marshal(document)
			if err != nil {
				t.Fatalf("marshal mutated fixture: %v", err)
			}
			if err := os.WriteFile(paths[index], data, 0o600); err != nil {
				t.Fatalf("write mutated fixture: %v", err)
			}
			digest := sha256.Sum256(data)
			args[7+index*2] = "sha256:" + hex.EncodeToString(digest[:])
		}
		if checkpoint["__corrupt_checkpoint_digest"] == true {
			args[7] = "sha256:" + strings.Repeat("0", 64)
		}
		if checkpoint["__symlink_environment"] == true {
			realEnvironment := paths[1] + ".real"
			if err := os.Rename(paths[1], realEnvironment); err != nil {
				t.Fatalf("rename environment fixture: %v", err)
			}
			if err := os.Symlink(realEnvironment, paths[1]); err != nil {
				t.Fatalf("symlink environment fixture: %v", err)
			}
		}
	}
	var stdout, stderr bytes.Buffer
	status := run(args, &stdout, &stderr)
	return stdout.Bytes(), stderr.String(), status
}
