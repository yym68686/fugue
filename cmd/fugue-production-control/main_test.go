package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/productioncontrol"
)

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
