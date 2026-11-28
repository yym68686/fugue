package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/productioncontrol"
)

func TestSignExecutionWitnessBindsExactObservedFileAndOriginalTime(t *testing.T) {
	now := time.Unix(1760000000, 0)
	d := func(letter string) string { return "sha256:" + strings.Repeat(letter, 64) }
	draft := productioncontrol.ExecutionWitness{
		Kind: productioncontrol.TerminalWitness, KeyID: "provider-observer", ClusterID: "cluster-a",
		ResourceID: productioncontrol.FrontWorkerResource("fugue-system", "edge-group-a"),
		Generation: 4, VerifiedDigest: d("a"), ObservationDigest: d("b"), AuthorityDigest: d("c"),
		ObservedAt: now.Add(-time.Minute).Unix(), IntentDigest: d("d"), ApprovalDigest: d("e"), Fence: 7,
		Outcome: productioncontrol.Applied,
	}
	raw, err := json.Marshal(draft)
	if err != nil {
		t.Fatal(err)
	}
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	pkcs8, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	jsonPath, keyPath := filepath.Join(dir, "draft.json"), filepath.Join(dir, "observer.pem")
	if err := os.WriteFile(jsonPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pkcs8}), 0o600); err != nil {
		t.Fatal(err)
	}
	reviewed := fmt.Sprintf("sha256:%x", sha256.Sum256(raw))
	var out bytes.Buffer
	args := []string{"sign-execution-witness", jsonPath, reviewed, keyPath, "2m"}
	if err := run(t.Context(), args, &out, func() time.Time { return now }); err != nil {
		t.Fatal(err)
	}
	var got productioncontrol.ExecutionWitness
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	input, err := got.SigningInput()
	if err != nil || !ed25519.Verify(pub, input, got.Signature) {
		t.Fatalf("output lacks a valid observer signature: %v", err)
	}
	canonical, _, err := productioncontrol.EncodeExecutionWitness(got)
	if err != nil || !bytes.Equal(canonical, out.Bytes()) || got.ObservedAt != draft.ObservedAt || got.ExpiresAt != now.Add(time.Minute).Unix() {
		t.Fatalf("output did not preserve observation time or canonical bytes: %v", err)
	}
	out.Reset()
	args[4] = "1m"
	if err := run(t.Context(), args, &out, func() time.Time { return now }); err == nil || out.Len() > 0 {
		t.Fatal("stale observation produced a signature")
	}
	args[4], args[2] = "2m", d("f")
	if err := run(t.Context(), args, &out, func() time.Time { return now }); err == nil || out.Len() > 0 {
		t.Fatal("unreviewed observation produced a signature")
	}
}

func TestOfflineApprovalRequiresExactReviewedSubmission(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1760000000, 0)
	clock := func() time.Time { return now }
	payload := []byte("exact immutable execution bundle")
	current, target := "sha256:"+strings.Repeat("a", 64), "sha256:"+strings.Repeat("c", 64)
	intent, err := productioncontrol.NewIntent("cluster-a", "kubernetes/demo/deployment/app", "request-1",
		productioncontrol.ComponentRelease, 9, current, target, current, payload)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(productioncontrol.Submission{Intent: intent, Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	submission := filepath.Join(dir, "submission.json")
	if err := os.WriteFile(submission, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	pkcs8, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	private := filepath.Join(dir, "owner.pem")
	if err := os.WriteFile(private, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pkcs8}), 0o600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := run(ctx, []string{"inspect", submission}, &out, clock); err != nil {
		t.Fatal(err)
	}
	var review struct {
		Digest string
		Intent productioncontrol.Intent
	}
	if err := json.Unmarshal(out.Bytes(), &review); err != nil || review.Intent != intent || !strings.HasPrefix(review.Digest, "sha256:") {
		t.Fatalf("inspection did not bind the intent: %+v, %v", review, err)
	}
	args := []string{"sign", submission, review.Digest, "owner-a", private, "10m"}
	expectRefusal := func() {
		t.Helper()
		out.Reset()
		if err := run(ctx, args, &out, clock); err == nil || out.Len() != 0 {
			t.Fatalf("invalid approval produced output or no error: %v", err)
		}
	}
	args[2] = current
	expectRefusal()
	args[2] = review.Digest
	args[5] = "25h"
	expectRefusal()
	args[5] = "10m"
	if err := os.Chmod(private, 0o644); err != nil {
		t.Fatal(err)
	}
	expectRefusal()
	if err := os.Chmod(private, 0o600); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(dir, "alias.pem")
	if err := os.Symlink(private, alias); err != nil {
		t.Fatal(err)
	}
	args[4] = alias
	expectRefusal()
	args[4] = private
	altered, err := json.Marshal(productioncontrol.Submission{Intent: intent, Payload: []byte("changed payload")})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(submission, altered, 0o600); err != nil {
		t.Fatal(err)
	}
	expectRefusal()
	if err := os.WriteFile(submission, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := run(ctx, args, &out, clock); err != nil {
		t.Fatal(err)
	}
	approval, err := productioncontrol.DecodeApproval(out.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	owners := productioncontrol.Approvers{"owner-a": {ClusterID: intent.ClusterID, PublicKey: pub}}
	if err := approval.Verify(intent, owners, now); err != nil {
		t.Fatal(err)
	}
}

func TestSignRegistryRehomeNoWriteBindsReviewedDraft(t *testing.T) {
	now := time.Unix(1760000000, 0)
	d := func(s string) string { return fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(s))) }
	proof := productioncontrol.RegistryRehomeNoWriteEvidence{
		ClusterID: "fixture", ResourceID: "kubernetes/fixture/deployment/example", IntentDigest: d("intent"), ApprovalDigest: d("approval"),
		Fence: 1, AdmissionDigest: d("admission"), ArchiveDigest: d("archive"), ReadbackDigest: d("readback"),
		EndpointFreeze: d("freeze"), WriterRevocation: d("revocation"), RequestDrain: d("drain"), ServingHealth: d("health"),
		ObservedAt: now.Add(-time.Minute).Unix(),
	}
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	pkcs8, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "observer.pem")
	draftPath := filepath.Join(dir, "closure.json")
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pkcs8}), 0o600); err != nil {
		t.Fatal(err)
	}
	draft, err := json.Marshal(proof)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(draftPath, draft, 0o600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	args := []string{"sign-registry-rehome-no-write", draftPath, d(string(draft)), keyPath, "2m"}
	clock := func() time.Time { return now }
	if err := run(t.Context(), args, &out, clock); err != nil {
		t.Fatal(err)
	}
	var got productioncontrol.RegistryRehomeNoWriteEvidence
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	message, err := got.SigningInput()
	if err != nil || !ed25519.Verify(pub, message, got.Signature) || got.ObservedAt != proof.ObservedAt || got.ExpiresAt != now.Add(time.Minute).Unix() {
		t.Fatalf("invalid signed closure: %v", err)
	}
	out.Reset()
	args[2] = d("different review")
	if err := run(t.Context(), args, &out, clock); err == nil || out.Len() != 0 {
		t.Fatal("unreviewed closure was signed")
	}
	duplicate := append(append([]byte(nil), draft[:len(draft)-1]...), []byte(`,"fence":1}`)...)
	if err := os.WriteFile(draftPath, duplicate, 0o600); err != nil {
		t.Fatal(err)
	}
	args[2] = d(string(duplicate))
	if err := run(t.Context(), args, &out, clock); err == nil || out.Len() != 0 {
		t.Fatal("ambiguous closure was signed")
	}
}
