package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func frontRepairReleaseCheckFixture(t *testing.T, source string, artifact frontRepairArtifactIdentity, now time.Time) (frontRepairReleasePolicy, frontRepairPreview, frontRepairApprovalEnvelope, ed25519.PrivateKey) {
	t.Helper()
	before, nodes, witnesses := partialFrontRollbackFixture()
	plan, err := planPartialFrontRollbackRepair(before, "authority-uid", "23", nodes, witnesses)
	if err != nil {
		t.Fatal(err)
	}
	digest := func(s string) string { return "sha256:" + strings.Repeat(s, 64) }
	preview := frontRepairPreview{Schema: frontRepairPreviewSchema, Plan: plan, Release: frontRepairReleaseBinding{
		Component: "edge-worker-fixture", Namespace: "fixture-system", GroupID: before.GroupID,
		PlanDigest: digest("1"), MonitorDigest: digest("2"), ArtifactDigest: digest("3"),
		WorkerSourceSHA: before.CurrentWorkerSourceSHA, WorkerImageDigest: before.CurrentWorkerImageDigest}}
	preview.Digest, err = preview.validatedDigest()
	if err != nil {
		t.Fatal(err)
	}
	pub, private, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	policy := frontRepairReleasePolicy{Schema: frontRepairReleasePolicySchema, Scope: "fixture-front-repair", Component: preview.Release.Component,
		Repository: "fixture/fugue", RepositoryID: artifact.RepositoryID, WorkflowID: 5, ExecutorCommit: source,
		VerifierKey: base64.RawURLEncoding.EncodeToString(pub)}
	envelope := frontRepairApprovalEnvelope{Schema: frontRepairApprovalEnvelopeSchema, Scope: policy.Scope, Artifact: artifact,
		Statement: frontRepairApproval{Schema: frontRepairApprovalSchema, PreviewDigest: preview.Digest, ExecutorCommit: source,
			ToolManifestDigest: digest("8"), ApprovedAt: now.Add(-time.Minute), ExpiresAt: now.Add(time.Minute)}}
	return policy, preview, signFrontRepairFixture(t, private, envelope), private
}

func TestFrontRepairReleaseGateRejectsBadApprovalBeforeAnyArtifactFetch(t *testing.T) {
	source := strings.Repeat("a", 40)
	_, identity, _, _, _, now := frontRepairActionsFixture(source, []byte("archive"))
	policy, preview, envelope, _ := frontRepairReleaseCheckFixture(t, source, identity, now)
	noReads := func(_ context.Context, endpoint string, _ int64) ([]byte, error) {
		t.Fatalf("unauthorized approval reached %s", endpoint)
		return nil, nil
	}
	for name, change := range map[string]func(*frontRepairReleasePolicy, *frontRepairPreview, *frontRepairApprovalEnvelope){
		"signature": func(_ *frontRepairReleasePolicy, _ *frontRepairPreview, e *frontRepairApprovalEnvelope) {
			e.Signature = ""
		},
		"repo": func(_ *frontRepairReleasePolicy, _ *frontRepairPreview, e *frontRepairApprovalEnvelope) {
			e.Artifact.RepositoryID++
		},
		"component": func(p *frontRepairReleasePolicy, _ *frontRepairPreview, _ *frontRepairApprovalEnvelope) {
			p.Component = "other-worker"
		},
		"cohort": func(_ *frontRepairReleasePolicy, p *frontRepairPreview, _ *frontRepairApprovalEnvelope) {
			p.Plan.Members = p.Plan.Members[1:]
		},
		"executor": func(p *frontRepairReleasePolicy, _ *frontRepairPreview, _ *frontRepairApprovalEnvelope) {
			p.ExecutorCommit = strings.Repeat("b", 40)
		},
		"public key": func(p *frontRepairReleasePolicy, _ *frontRepairPreview, _ *frontRepairApprovalEnvelope) {
			p.VerifierKey += "\n"
		},
	} {
		t.Run(name, func(t *testing.T) {
			p, v, e := policy, preview, envelope
			change(&p, &v, &e)
			if result, err := qualifyFrontRepairRelease(context.Background(), p, e, v, noReads, now); err == nil || result.intent.AuthorizationDigest != "" {
				t.Fatalf("unqualified approval was accepted: %v", err)
			}
		})
	}
	if _, err := qualifyFrontRepairRelease(context.Background(), policy, envelope, preview, noReads, envelope.Statement.ExpiresAt); err == nil {
		t.Fatal("expired approval attempted to enter release gate")
	}
}

func TestFrontRepairReleaseCheckQualifiesSignedPreviewAndExactCrossBuiltArtifacts(t *testing.T) {
	source, manifest, raw, dirty := frontRepairBuildFixtureArchive(t)
	_, identity, run, job, record, now := frontRepairActionsFixture(source, raw)
	policy, preview, envelope, key := frontRepairReleaseCheckFixture(t, source, identity, now)
	envelope.Statement.ToolManifestDigest = manifest.Digest
	envelope = signFrontRepairFixture(t, key, envelope)
	dir := t.TempDir()
	write := func(name string, value any) string {
		t.Helper()
		path := filepath.Join(dir, name)
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	args := []string{"--policy", write("policy.json", policy), "--preview", write("preview.json", preview), "--approval", write("approval.json", envelope)}
	var reads []string
	const prefix = "repos/fixture/fugue/actions/"
	get := func(_ context.Context, endpoint string, _ int64) ([]byte, error) {
		reads = append(reads, endpoint)
		switch endpoint {
		case prefix + "runs/10/attempts/2":
			return json.Marshal(run)
		case prefix + "jobs/11":
			return json.Marshal(job)
		case prefix + "artifacts/12":
			return json.Marshal(record)
		case prefix + "artifacts/12/zip":
			return raw, nil
		default:
			return nil, errors.New("unexpected fixture endpoint")
		}
	}
	var out bytes.Buffer
	err := runFrontRepairReleaseCheckWithFetcher(args, &out, get, now)
	if dirty {
		if err == nil || out.Len() != 0 {
			t.Fatal("dirty tool builds received a qualification receipt")
		}
		return
	}
	var receipt frontRepairReleaseCheckReceipt
	if err != nil || decodeStrictJSON(out.Bytes(), &receipt) != nil || receipt.PreviewDigest != preview.Digest ||
		receipt.Group != preview.Release.GroupID || receipt.MemberCount != len(preview.Plan.Members) ||
		receipt.Artifact != identity || receipt.TargetGeneration != preview.Plan.TargetGeneration ||
		!exactSHA256Digest(receipt.AuthorizationDigest) || !reflect.DeepEqual(reads, []string{
		prefix + "runs/10/attempts/2", prefix + "jobs/11", prefix + "artifacts/12", prefix + "artifacts/12/zip"}) {
		t.Fatalf("signed release check failed: %v; reads=%v", err, reads)
	}
	out.Reset()
	envelope.Statement.ToolManifestDigest = shaDigest([]byte("other signed tool manifest"))
	write("approval.json", signFrontRepairFixture(t, key, envelope))
	if err := runFrontRepairReleaseCheckWithFetcher(args, &out, get, now); err == nil || out.Len() != 0 {
		t.Fatal("verified ZIP not matching the signed tool manifest got a receipt")
	}
	if err := runFrontRepairReleaseCheckWithFetcher(append(args, "--apply"), &out, get, now); err == nil {
		t.Fatal("release check accepted an apply option")
	}
}
