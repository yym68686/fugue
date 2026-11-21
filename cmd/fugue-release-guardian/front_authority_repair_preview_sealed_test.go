package main

import (
	"bytes"
	"crypto/ecdh"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"strings"
	"testing"
	"time"
)

func TestFrontRepairSealedPreviewBindsExactRunAndRecipient(t *testing.T) {
	source := strings.Repeat("a", 40)
	_, identity, _, _, _, _ := frontRepairActionsFixture(source, []byte("archive"))
	_, preview, _, _ := frontRepairReleaseCheckFixture(t, source, identity, time.Now().UTC())
	s := frontRepairPreviewSealScope{Repository: "fixture/fugue", Declaration: strings.Repeat("b", 40), Executor: strings.Repeat("a", 40), Nonce: strings.Repeat("c", 64), RunID: 30, Attempt: 2}
	recipient, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	pemBytes, err := x509.MarshalPKIXPublicKey(recipient.PublicKey())
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := frontRepairRecipient(string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pemBytes})))
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := sealFrontRepairPreview(s, preview, parsed)
	if err != nil {
		t.Fatal(err)
	}
	public, err := json.Marshal(sealed)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(public, []byte(preview.Plan.Members[0].Node.UID)) && len(preview.Plan.Members[0].Node.UID) > 12 {
		t.Fatal("public preview artifact leaked the reviewed Node identity")
	}
	if bytes.Contains(public, []byte(preview.Digest)) || bytes.Contains(public, []byte("\"Members\"")) {
		t.Fatal("public preview artifact contains private predecessor content")
	}
	got, err := openFrontRepairPreview(sealed, s, recipient)
	if err != nil || got.Digest != preview.Digest {
		t.Fatalf("could not restore exact preview: %v", err)
	}
	wrong, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := openFrontRepairPreview(sealed, s, wrong); err == nil {
		t.Fatal("foreign key recovered the preview")
	}
	renamed := sealed
	renamed.Scope.Attempt++
	if _, err := openFrontRepairPreview(renamed, renamed.Scope, recipient); err == nil {
		t.Fatal("a different outer run was not rejected by the bound inner scope")
	}
	sealed.Ciphertext[len(sealed.Ciphertext)-1] ^= 1
	if _, err := openFrontRepairPreview(sealed, s, recipient); err == nil {
		t.Fatal("altered sealed preview was accepted")
	}
}
