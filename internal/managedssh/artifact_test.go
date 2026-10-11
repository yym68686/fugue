package managedssh

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"testing"
)

func TestManagedSSHArtifactRejectsTamperingAndBadCandidateRetainsState(test *testing.T) {
	configuration, hostSigner, _ := testConfiguration(test)
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		test.Fatal(err)
	}
	artifact, err := SignArtifact(configuration, "config-key", privateKey)
	if err != nil {
		test.Fatal(err)
	}
	keys := map[string]ed25519.PublicKey{"config-key": publicKey}
	verified, err := VerifyArtifact(artifact, keys)
	if err != nil {
		test.Fatal(err)
	}
	server, err := NewServer(&testExecutor{}, &testAudit{})
	if err != nil {
		test.Fatal(err)
	}
	if err := server.Activate(context.Background(), verified, hostSigner); err != nil {
		test.Fatal(err)
	}
	before := server.ConfigurationDigest()
	for name, mutate := range map[string]func(*Artifact){
		"workload":         func(value *Artifact) { value.Configuration.Target.AppID = "other" },
		"generation":       func(value *Artifact) { value.Configuration.Generation++ },
		"digest":           func(value *Artifact) { value.Digest = "sha256:wrong" },
		"signing identity": func(value *Artifact) { value.KeyID = "unknown" },
		"unsigned":         func(value *Artifact) { value.Signature = "" },
	} {
		test.Run(name, func(test *testing.T) {
			changed := artifact
			mutate(&changed)
			if _, err := VerifyArtifact(changed, keys); !errors.Is(err, ErrArtifact) {
				test.Fatal("invalid signed state accepted", err)
			}
		})
	}
	if server.ConfigurationDigest() != before {
		test.Fatal("verification changed serving state")
	}
}
