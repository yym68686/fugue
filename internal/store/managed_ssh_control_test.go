package store

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/managedssh"
	"fugue/internal/model"
)

func TestManagedSSHControlPersistsArtifactsBeforeActivation(test *testing.T) {
	stateStore, app, _ := setupSSHIdentityStore(test)
	identity, err := stateStore.PrepareAppSSHIdentity(app.TenantID, app.ID)
	if err != nil {
		test.Fatal(err)
	}
	if _, err := stateStore.PutManagedSSHIntent(app.TenantID, app.ID, 0, managedSSHSpec(identity.ID)); err != nil {
		test.Fatal(err)
	}
	verifyManagedSSHControlStore(test, stateStore, app, identity)
}

func verifyManagedSSHControlStore(test *testing.T, stateStore *Store, app model.App, identity model.AppSSHIdentity) {
	test.Helper()
	intent, err := stateStore.GetManagedSSHIntent(app.TenantID, app.ID)
	if err != nil {
		test.Fatal(err)
	}
	now := time.Now().UTC()
	target := managedssh.Target{TenantID: app.TenantID, AppID: app.ID, Namespace: "workloads", PodName: "workspace", PodUID: "pod-uid", ContainerID: "containerd://container-id", Container: intent.Spec.Container, User: intent.Spec.User, WorkingDirectory: intent.Spec.WorkingDirectory}
	configuration, err := managedssh.CompileConfiguration(intent, identity, target, nil, 1, now)
	if err != nil {
		test.Fatal(err)
	}
	signingKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{89}, ed25519.SeedSize))
	artifact, err := managedssh.SignArtifact(configuration, "config-signer", signingKey)
	if err != nil {
		test.Fatal(err)
	}
	release := managedssh.RuntimeRelease{Image: "registry.example/ssh@sha256:" + strings.Repeat("a", 64), HelperDigest: "sha256:" + strings.Repeat("b", 64), Architecture: "amd64"}
	slot := &managedssh.ServingSlot{ID: managedssh.SlotID(identity.ID, target, release), Namespace: "platform-ssh", Release: release, Artifact: artifact, CreatedAt: now}
	state := managedssh.ControlState{Schema: "fugue.managed-ssh-control/v1", TenantID: app.TenantID, AppID: app.ID, IdentityID: identity.ID, Revision: 1, Generation: 1, IntentGeneration: intent.Generation, Candidate: slot}
	if err := stateStore.SaveManagedSSHControl(0, state); err != nil {
		test.Fatal(err)
	}
	if err := stateStore.SaveManagedSSHControl(0, state); !errors.Is(err, ErrConflict) {
		test.Fatal("stale publisher overwrote control state", err)
	}
	stored, err := stateStore.GetManagedSSHControl(app.TenantID, app.ID)
	if err != nil || !reflect.DeepEqual(stored, state) {
		test.Fatal("candidate did not roundtrip", err)
	}
	if _, err := stateStore.GetManagedSSHControl("other", app.ID); !errors.Is(err, ErrNotFound) {
		test.Fatal("control state crossed tenant boundary", err)
	}
	if _, err := state.ActivateCandidate(artifact.Digest, identity.Fingerprint, "legacy-route", "reviewer", now); !errors.Is(err, managedssh.ErrCandidateUnverified) {
		test.Fatal("unobserved candidate activated", err)
	}
	slot.Evidence = &managedssh.CandidateEvidence{ArtifactDigest: artifact.Digest, IdentityID: identity.ID, Fingerprint: identity.Fingerprint, ServerPodUID: "server-pod-uid", TargetPodUID: target.PodUID, TargetContainerID: target.ContainerID, KeyExchangeVerified: true, RuntimeVerified: true, ObservedAt: now, ExpiresAt: now.Add(time.Minute)}
	state.Revision++
	if err := stateStore.SaveManagedSSHControl(1, state); err != nil {
		test.Fatal(err)
	}
	activated, err := state.ActivateCandidate(artifact.Digest, identity.Fingerprint, "legacy-route", "reviewer", now)
	if err != nil {
		test.Fatal(err)
	}
	if err := stateStore.SaveManagedSSHControl(2, activated); err != nil {
		test.Fatal(err)
	}
	if activated.Active == nil || activated.Candidate != nil || activated.RouteGeneration != 1 {
		test.Fatal("incorrect activation state")
	}
	mutated := *activated.Active
	mutated.Artifact.Configuration.Keys = append(mutated.Artifact.Configuration.Keys, managedssh.KeyGrant{ID: "unreviewed", PublicKey: identity.PublicKey, ExpiresAt: configuration.ValidUntil})
	mutated.Artifact, err = managedssh.SignArtifact(mutated.Artifact.Configuration, "config-signer", signingKey)
	if err != nil {
		test.Fatal(err)
	}
	bad := activated
	bad.Revision++
	bad.Active = &mutated
	if err := stateStore.SaveManagedSSHControl(3, bad); !errors.Is(err, ErrConflict) {
		test.Fatal("immutable artifact generation overwritten", err)
	}
	actual, err := stateStore.GetManagedSSHControl(app.TenantID, app.ID)
	if err != nil || actual.Active.Artifact.Digest != artifact.Digest || actual.Revision != 3 {
		test.Fatal("rejected publication damaged serving state", err)
	}
	updated, err := stateStore.PutManagedSSHIntent(app.TenantID, app.ID, intent.Generation, intent.Spec)
	if err != nil {
		test.Fatal(err)
	}
	staleIntent := activated
	staleIntent.Revision++
	if err := stateStore.SaveManagedSSHControl(3, staleIntent); !errors.Is(err, ErrConflict) {
		test.Fatal("publisher ignored changed intent", err)
	}
	staleIntent.IntentGeneration = updated.Generation
	if err := stateStore.SaveManagedSSHControl(3, staleIntent); err != nil {
		test.Fatal(err)
	}
}
