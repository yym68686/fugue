package managedssh

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
)

func TestManagedSSHLoaderPersistsVerifiedStateAndRejectsReplay(test *testing.T) {
	configuration, _, _ := testConfiguration(test)
	hostPublic, hostPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		test.Fatal(err)
	}
	sshPublic, err := ssh.NewPublicKey(hostPublic)
	if err != nil {
		test.Fatal(err)
	}
	configuration.Identity.PublicKey = string(ssh.MarshalAuthorizedKey(sshPublic))
	configuration.Identity.PublicKey = configuration.Identity.PublicKey[:len(configuration.Identity.PublicKey)-1]
	configuration.Identity.Fingerprint = ssh.FingerprintSHA256(sshPublic)
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		test.Fatal(err)
	}
	root := test.TempDir()
	server, err := NewServer(&testExecutor{}, &testAudit{})
	if err != nil {
		test.Fatal(err)
	}
	loader := Loader{Server: server, ArtifactPath: filepath.Join(root, "candidate.json"), TrustPath: filepath.Join(root, "trust.json"), PrivateKeyPath: filepath.Join(root, "host-key"), LKGPath: filepath.Join(root, "lkg.json")}
	writeJSON := func(path string, value any) {
		test.Helper()
		raw, err := json.Marshal(value)
		if err != nil {
			test.Fatal(err)
		}
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			test.Fatal(err)
		}
	}
	writeJSON(loader.TrustPath, map[string]any{"schema": "fugue.managed-ssh-trust/v1", "keys": map[string]string{"configuration": base64.StdEncoding.EncodeToString(public)}})
	privateBlock, err := ssh.MarshalPrivateKey(hostPrivate, "")
	if err != nil {
		test.Fatal(err)
	}
	if err := os.WriteFile(loader.PrivateKeyPath, pem.EncodeToMemory(privateBlock), 0o600); err != nil {
		test.Fatal(err)
	}
	first, err := SignArtifact(configuration, "configuration", private)
	if err != nil {
		test.Fatal(err)
	}
	writeJSON(loader.ArtifactPath, first)
	if err := loader.Load(context.Background(), false); err != nil {
		test.Fatal(err)
	}
	firstDigest := server.ConfigurationDigest()
	invalid := first
	invalid.Signature = "invalid"
	writeJSON(loader.ArtifactPath, invalid)
	if err := loader.Load(context.Background(), false); !errors.Is(err, ErrArtifact) {
		test.Fatal("unsigned replacement accepted", err)
	}
	if server.ConfigurationDigest() != firstDigest {
		test.Fatal("invalid file replaced serving state")
	}
	configuration.Generation++
	second, err := SignArtifact(configuration, "configuration", private)
	if err != nil {
		test.Fatal(err)
	}
	writeJSON(loader.ArtifactPath, second)
	if err := loader.Load(context.Background(), false); err != nil {
		test.Fatal(err)
	}
	secondDigest := server.ConfigurationDigest()
	fresh, err := NewServer(&testExecutor{}, &testAudit{})
	if err != nil {
		test.Fatal(err)
	}
	loader.Server = fresh
	writeJSON(loader.ArtifactPath, first)
	if err := loader.Load(context.Background(), false); !errors.Is(err, ErrGeneration) {
		test.Fatal("restart accepted stale candidate", err)
	}
	if err := loader.Load(context.Background(), true); err != nil {
		test.Fatal(err)
	}
	if fresh.ConfigurationDigest() != secondDigest {
		test.Fatal("restart lost verified generation")
	}
	configuration.Generation++
	configuration.ValidUntil = time.Now().Add(-time.Second)
	expired, err := SignArtifact(configuration, "configuration", private)
	if err != nil {
		test.Fatal(err)
	}
	writeJSON(loader.ArtifactPath, expired)
	if err := loader.Load(context.Background(), false); !errors.Is(err, ErrConfiguration) {
		test.Fatal("expired authorization artifact accepted", err)
	}
	if fresh.ConfigurationDigest() != secondDigest {
		test.Fatal("expired artifact replaced serving state")
	}
}

func TestManagedSSHAuditFailureDoesNotActivateCandidate(test *testing.T) {
	configuration, hostSigner, _ := testConfiguration(test)
	audit := &testAudit{}
	server, err := NewServer(&testExecutor{}, audit)
	if err != nil {
		test.Fatal(err)
	}
	if err := server.Activate(context.Background(), configuration, hostSigner); err != nil {
		test.Fatal(err)
	}
	before := server.ConfigurationDigest()
	audit.failure = true
	configuration.Generation++
	if err := server.Activate(context.Background(), configuration, hostSigner); err == nil {
		test.Fatal("audit failure ignored")
	}
	if server.ConfigurationDigest() != before {
		test.Fatal("failed activation changed serving state")
	}
	audit.failure = false
	if err := server.activate(context.Background(), configuration, hostSigner, func() error { return errors.New("disk full") }); err == nil {
		test.Fatal("failed persistence accepted")
	}
	if server.ConfigurationDigest() != before {
		test.Fatal("persistence failure changed serving state")
	}
}
