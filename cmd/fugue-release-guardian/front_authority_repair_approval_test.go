package main

import (
	"crypto/ed25519"
	"encoding/base64"
	"strings"
	"testing"
	"time"
)

func signFrontRepairFixture(t *testing.T, key ed25519.PrivateKey, envelope frontRepairApprovalEnvelope) frontRepairApprovalEnvelope {
	t.Helper()
	raw, err := envelope.signingBytes()
	if err != nil {
		t.Fatal(err)
	}
	envelope.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, raw))
	return envelope
}

func TestFrontRepairRequiresSignedScopeExactArtifactAndLiveApproval(t *testing.T) {
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
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	_, wrongSigner, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 2, 9, 12, 0, 0, 0, time.UTC)
	executor := strings.Repeat("a", 40)
	build := frontRepairArtifactIdentity{RepositoryID: 1, WorkflowRunID: 2, ProducerJobID: 25, RunAttempt: 1, ArtifactID: 3, ArchiveDigest: digest("4")}
	trust := frontRepairApprovalTrust{Scope: "fixture-protected", PublicKey: pub, ExecutorCommit: executor, Artifact: build}
	tools := frontRepairToolSet{digest: digest("5"), source: executor, tools: map[string]frontRepairTool{"amd64": {}}}
	base := frontRepairApprovalEnvelope{Schema: frontRepairApprovalEnvelopeSchema, Scope: trust.Scope, Artifact: build,
		Statement: frontRepairApproval{Schema: frontRepairApprovalSchema, PreviewDigest: preview.Digest, ExecutorCommit: executor,
			ToolManifestDigest: tools.digest, ApprovedAt: now.Add(-time.Minute), ExpiresAt: now.Add(time.Minute)}}
	signed := signFrontRepairFixture(t, priv, base)
	intent, err := signed.verify(preview, trust, tools, now)
	if err != nil || intent.Group != plan.Before.GroupID || len(intent.Nodes) != len(nodes) || !exactSHA256Digest(intent.AuthorizationDigest) {
		t.Fatalf("valid signed intent failed: %+v err=%v", intent, err)
	}

	for _, tt := range []struct {
		name   string
		change func(*frontRepairApprovalEnvelope, *frontRepairApprovalTrust, *frontRepairToolSet)
	}{
		{"unsigned", func(e *frontRepairApprovalEnvelope, _ *frontRepairApprovalTrust, _ *frontRepairToolSet) {
			e.Signature = ""
		}},
		{"different signer", func(e *frontRepairApprovalEnvelope, _ *frontRepairApprovalTrust, _ *frontRepairToolSet) {
			*e = signFrontRepairFixture(t, wrongSigner, *e)
		}},
		{"scope substitution", func(e *frontRepairApprovalEnvelope, tr *frontRepairApprovalTrust, _ *frontRepairToolSet) {
			e.Scope, tr.Scope = "other-scope", "other-scope"
		}},
		{"run attempt substitution", func(_ *frontRepairApprovalEnvelope, tr *frontRepairApprovalTrust, _ *frontRepairToolSet) {
			tr.Artifact.RunAttempt++
		}},
		{"archive substitution", func(_ *frontRepairApprovalEnvelope, tr *frontRepairApprovalTrust, _ *frontRepairToolSet) {
			tr.Artifact.ArchiveDigest = digest("6")
		}},
		{"changed executor", func(_ *frontRepairApprovalEnvelope, tr *frontRepairApprovalTrust, ti *frontRepairToolSet) {
			tr.ExecutorCommit, ti.source = strings.Repeat("b", 40), strings.Repeat("b", 40)
		}},
		{"changed tool manifest", func(_ *frontRepairApprovalEnvelope, _ *frontRepairApprovalTrust, ti *frontRepairToolSet) {
			ti.digest = digest("6")
		}},
		{"unsigned deadline extension", func(e *frontRepairApprovalEnvelope, _ *frontRepairApprovalTrust, _ *frontRepairToolSet) {
			e.Statement.ExpiresAt = now.Add(time.Hour)
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			e, tr, ti := signed, trust, tools
			tt.change(&e, &tr, &ti)
			if rejected, err := e.verify(preview, tr, ti, now); err == nil || rejected.AuthorizationDigest != "" {
				t.Fatalf("unauthorized repair: %+v err=%v", rejected, err)
			}
		})
	}
	if _, err := signed.verify(preview, trust, tools, base.Statement.ExpiresAt); err == nil {
		t.Fatal("expired approval executed")
	}
	renewed := base
	renewed.Statement.ApprovedAt = now.Add(time.Minute)
	renewed.Statement.ExpiresAt = now.Add(2 * time.Minute)
	retry, err := signFrontRepairFixture(t, priv, renewed).verify(preview, trust, tools, now.Add(time.Minute))
	if err != nil || retry.AuthorizationDigest != intent.AuthorizationDigest {
		t.Fatalf("exact signed renewal cannot resume the journal: %+v err=%v", retry, err)
	}
	tr2 := trust
	tr2.Artifact.ArtifactID++
	renewed.Artifact = tr2.Artifact
	replacement, err := signFrontRepairFixture(t, priv, renewed).verify(preview, tr2, tools, now.Add(time.Minute))
	if err != nil || replacement.AuthorizationDigest == intent.AuthorizationDigest {
		t.Fatalf("replacement artifact could resume the earlier journal: %+v err=%v", replacement, err)
	}
}
