package sshcontrol

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

	"fugue/internal/managedssh"
	"fugue/internal/model"
	"fugue/internal/sshidentity"
	"fugue/internal/store"
	"golang.org/x/crypto/ssh"
)

type testRuntime struct {
	store        *store.Store
	target       managedssh.Target
	release      managedssh.RuntimeRelease
	resolveError error
	observeError error
	applied      []managedssh.ServingSlot
	now          time.Time
}

func (runtime *testRuntime) Resolve(context.Context, model.App, model.ManagedSSHIntent, managedssh.Target) (managedssh.Target, managedssh.RuntimeRelease, error) {
	return runtime.target, runtime.release, runtime.resolveError
}

func (runtime *testRuntime) Apply(ctx context.Context, slot managedssh.ServingSlot, private []byte, trust map[string]ed25519.PublicKey) error {
	configuration, err := managedssh.VerifyArtifact(slot.Artifact, trust)
	if err != nil {
		return err
	}
	signer, err := ssh.ParsePrivateKey(private)
	if err != nil || ssh.FingerprintSHA256(signer.PublicKey()) != configuration.Identity.Fingerprint {
		return errors.New("private identity does not match artifact")
	}
	state, err := runtime.store.GetManagedSSHControl(configuration.Identity.TenantID, configuration.Identity.AppID)
	if err != nil {
		return err
	}
	found := false
	for _, stored := range state.Slots() {
		if stored.Artifact.Digest == slot.Artifact.Digest {
			found = true
		}
	}
	if !found {
		return errors.New("runtime apply happened before durable artifact publication")
	}
	runtime.applied = append(runtime.applied, slot)
	return nil
}

func (runtime *testRuntime) Observe(ctx context.Context, slot managedssh.ServingSlot) (managedssh.CandidateEvidence, error) {
	if runtime.observeError != nil {
		return managedssh.CandidateEvidence{}, runtime.observeError
	}
	configuration := slot.Artifact.Configuration
	return managedssh.CandidateEvidence{ArtifactDigest: slot.Artifact.Digest, IdentityID: configuration.Identity.ID, Fingerprint: configuration.Identity.Fingerprint, ServerPodUID: "server-pod", TargetPodUID: configuration.Target.PodUID, TargetContainerID: configuration.Target.ContainerID, KeyExchangeVerified: true, RuntimeVerified: true, ObservedAt: runtime.now, ExpiresAt: runtime.now.Add(time.Minute)}, nil
}

func TestReconcileStagesCandidateAndRevokesDespiteFailedTarget(test *testing.T) {
	ctx := context.Background()
	stateStore := store.New(filepath.Join(test.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		test.Fatal(err)
	}
	tenant, err := stateStore.CreateTenant("managed-ssh-test")
	if err != nil {
		test.Fatal(err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "workspaces", "")
	if err != nil {
		test.Fatal(err)
	}
	app, err := stateStore.CreateApp(tenant.ID, project.ID, "workspace", "", model.AppSpec{Image: "registry.example/workspace:v1", Replicas: 1})
	if err != nil {
		test.Fatal(err)
	}
	keyring, err := sshidentity.NewKeyring("identity-storage", map[string][]byte{"identity-storage": bytes.Repeat([]byte{53}, 32)})
	if err != nil {
		test.Fatal(err)
	}
	stateStore.ConfigureSSHIdentityKeyring(keyring)
	identity, err := stateStore.PrepareAppSSHIdentity(tenant.ID, app.ID)
	if err != nil {
		test.Fatal(err)
	}
	userIdentity, err := keyring.Create(tenant.ID, "ssh-user", time.Now())
	if err != nil {
		test.Fatal(err)
	}
	userKey, err := stateStore.CreateSSHKey(tenant.ID, "user", userIdentity.Identity.PublicKey)
	if err != nil {
		test.Fatal(err)
	}
	spec := model.ManagedSSHIntentSpec{Enabled: true, IdentityID: identity.ID, Container: "workspace", User: "app", WorkingDirectory: "/workspace", AuthorizedKeyIDs: []string{userKey.ID}, AllowPTY: true, AllowSFTP: true, MaxConnections: 8, MaxConnectionsPerIP: 4, MaxChannels: 4}
	if _, err := stateStore.PutManagedSSHIntent(tenant.ID, app.ID, 0, spec); err != nil {
		test.Fatal(err)
	}
	keyFile := filepath.Join(test.TempDir(), "signer.json")
	raw, _ := json.Marshal(map[string]any{"schema": "fugue.managed-ssh-signing/v1", "active_key_id": "config-key", "keys": map[string]string{"config-key": base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{81}, 32))}})
	if err := os.WriteFile(keyFile, raw, 0o600); err != nil {
		test.Fatal(err)
	}
	now := time.Now().UTC()
	runtime := &testRuntime{store: stateStore, now: now, target: managedssh.Target{TenantID: tenant.ID, AppID: app.ID, Namespace: "workloads", PodName: "workspace", PodUID: "pod-current", Container: spec.Container, ContainerID: "container-current", User: spec.User, WorkingDirectory: spec.WorkingDirectory}, release: managedssh.RuntimeRelease{Image: "registry.example/ssh@sha256:" + strings.Repeat("a", 64), HelperDigest: "sha256:" + strings.Repeat("b", 64), Architecture: "amd64"}}
	reconciler := &Reconciler{Store: stateStore, Runtime: runtime, Namespace: "platform-ssh", SigningKeyFile: keyFile, Now: func() time.Time { return runtime.now }}
	if err := reconciler.Reconcile(ctx, tenant.ID, app.ID); err != nil {
		test.Fatal(err)
	}
	state, err := stateStore.GetManagedSSHControl(tenant.ID, app.ID)
	if err != nil || state.Active != nil || state.Candidate == nil || !state.Candidate.Verified(now) {
		test.Fatal("initial candidate activated public route or missed evidence", err)
	}
	if len(runtime.applied) != 1 || len(runtime.applied[0].Artifact.Configuration.Keys) != 1 {
		test.Fatal("candidate did not load current key grant")
	}
	if _, err := stateStore.GetAppSSHEndpoint(app.ID); !errors.Is(err, store.ErrNotFound) {
		test.Fatal("preparing candidate allocated public route", err)
	}
	activated, err := state.ActivateCandidate(state.Candidate.Artifact.Digest, identity.Fingerprint, "legacy-route", "reviewer", now)
	if err != nil {
		test.Fatal(err)
	}
	if err := stateStore.SaveManagedSSHControl(state.Revision, activated); err != nil {
		test.Fatal(err)
	}
	activeID := activated.Active.ID
	if _, err := stateStore.DeleteSSHKey(userKey.ID); err != nil {
		test.Fatal(err)
	}
	runtime.resolveError = errors.New("target discovery unavailable")
	if err := reconciler.Reconcile(ctx, tenant.ID, app.ID); err == nil {
		test.Fatal("target failure not surfaced")
	}
	state, err = stateStore.GetManagedSSHControl(tenant.ID, app.ID)
	if err != nil || state.Active.ID != activeID || len(state.Active.Artifact.Configuration.Keys) != 0 || state.Active.Artifact.Configuration.Identity != identity {
		test.Fatal("discovery failure prevented revocation or replaced identity", err)
	}
	lastApplied := runtime.applied[len(runtime.applied)-1]
	if len(lastApplied.Artifact.Configuration.Keys) != 0 || lastApplied.ID != activeID {
		test.Fatal("revocation was not delivered during target failure")
	}
	runtime.resolveError = nil
	runtime.target.PodUID, runtime.target.ContainerID, runtime.target.PodName = "replacement-pod", "replacement-container", "workspace-new"
	runtime.observeError = errors.New("candidate handshake failed")
	if err := reconciler.Reconcile(ctx, tenant.ID, app.ID); err == nil {
		test.Fatal("failed candidate reported success")
	}
	state, err = stateStore.GetManagedSSHControl(tenant.ID, app.ID)
	if err != nil || state.Active.ID != activeID || state.Candidate == nil || state.Candidate.Verified(now) {
		test.Fatal("failed candidate displaced active slot", err)
	}
	after, err := stateStore.GetApp(app.ID)
	if err != nil || !reflect.DeepEqual(after.Spec, app.Spec) {
		test.Fatal("SSH reconciliation edited application spec", err)
	}
}
