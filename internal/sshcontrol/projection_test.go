package sshcontrol

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/managedssh"
	"fugue/internal/model"
	"fugue/internal/sshidentity"
	"fugue/internal/store"
)

// projectedRuntime separates Secret publication from the loaded
// runtime snapshot, as the real Kubernetes volume projection does.
type projectedRuntime struct {
	*testRuntime
	loaded       map[string]managedssh.ServingSlot
	pending      map[string]managedssh.ServingSlot
	readyAt      map[string]time.Time
	delay        time.Duration
	restartStore func() *store.Store
}

func (r *projectedRuntime) Apply(ctx context.Context, slot managedssh.ServingSlot, key []byte, trust map[string]ed25519.PublicKey) error {
	if err := r.testRuntime.Apply(ctx, slot, key, trust); err != nil {
		return err
	}
	if r.loaded == nil {
		r.loaded = map[string]managedssh.ServingSlot{}
		r.pending = map[string]managedssh.ServingSlot{}
		r.readyAt = map[string]time.Time{}
	}
	if r.loaded[slot.ID].ID == "" {
		r.loaded[slot.ID] = slot
		return nil
	}
	if r.pending[slot.ID].Artifact.Digest != slot.Artifact.Digest {
		r.pending[slot.ID] = slot
		r.readyAt[slot.ID] = r.now.Add(r.delay)
	}
	if !r.now.Before(r.readyAt[slot.ID]) {
		r.loaded[slot.ID] = r.pending[slot.ID]
	}
	return nil
}
func (r *projectedRuntime) Observe(ctx context.Context, slot managedssh.ServingSlot) (managedssh.CandidateEvidence, error) {
	return r.testRuntime.Observe(ctx, r.loaded[slot.ID])
}

func projectionFixture(test *testing.T) (*Reconciler, *projectedRuntime, string, string) {
	test.Helper()
	ctx := context.Background()
	storePath := filepath.Join(test.TempDir(), "store.json")
	stateStore := store.New(storePath)
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

	delayed := &projectedRuntime{testRuntime: runtime, delay: 30 * time.Second}
	delayed.restartStore = func() *store.Store {
		reopened := store.New(storePath)
		if err := reopened.Init(); err != nil {
			test.Fatal(err)
		}
		reopened.ConfigureSSHIdentityKeyring(keyring)
		return reopened
	}
	reconciler.Runtime = delayed
	if err := reconciler.Reconcile(ctx, tenant.ID, app.ID); err != nil {
		test.Fatal(err)
	}
	state, err := stateStore.GetManagedSSHControl(tenant.ID, app.ID)
	if err != nil {
		test.Fatal(err)
	}
	active, err := state.ActivateCandidate(state.Candidate.Artifact.Digest, identity.Fingerprint, "old-route", "operator", runtime.now)
	if err != nil {
		test.Fatal(err)
	}
	if err = stateStore.SaveManagedSSHControl(state.Revision, active); err != nil {
		test.Fatal(err)
	}
	return reconciler, delayed, tenant.ID, app.ID
}

func TestProjectedAuthorizationWaitsAcrossReconcileAndRestart(test *testing.T) {
	ctx := context.Background()
	reconciler, delayed, tenantID, appID := projectionFixture(test)
	runtime, stateStore := delayed.testRuntime, reconciler.Store
	// Refresh the old artifact's receipt while it is still the desired one.
	runtime.now = runtime.now.Add(40 * time.Second)
	if err := reconciler.Reconcile(ctx, tenantID, appID); err != nil {
		test.Fatal(err)
	}
	state, err := stateStore.GetManagedSSHControl(tenantID, appID)
	if err != nil {
		test.Fatal(err)
	}
	previous := *state.Active
	runtime.now = runtime.now.Add(40 * time.Second)
	if !previous.Verified(runtime.now) {
		test.Fatal("precondition: old artifact must have fresh positive evidence")
	}
	err = reconciler.Reconcile(ctx, tenantID, appID)
	if err != nil {
		test.Fatal("healthy projection delay must remain pending", err)
	}
	state, err = stateStore.GetManagedSSHControl(tenantID, appID)
	if err != nil {
		test.Fatal(err)
	}
	status := state.Active.PublicStatus(runtime.now)
	if state.Active.Verified(runtime.now) || !status.Serving || status.ConvergenceState != "pending_apply" || state.Active.LastValidEvidence == nil || state.Active.LoadedArtifact.Digest != previous.Artifact.Digest {
		test.Fatal("desired and serving evidence were conflated", status)
	}
	deadline, desired := *state.Active.PendingApply, state.Active.Artifact.Digest
	// Reload the persisted JSON through a fresh store/controller instance.
	reconciler = &Reconciler{Store: delayed.restartStore(), Runtime: delayed, Namespace: reconciler.Namespace, SigningKeyFile: reconciler.SigningKeyFile, Now: reconciler.Now}
	stateStore, runtime.store = reconciler.Store, reconciler.Store
	runtime.now = runtime.now.Add(10 * time.Second)
	if err := reconciler.Reconcile(ctx, tenantID, appID); err != nil {
		test.Fatal(err)
	}
	state, err = stateStore.GetManagedSSHControl(tenantID, appID)
	if err != nil || state.Active.Artifact.Digest != desired || *state.Active.PendingApply != deadline {
		test.Fatal("retry reset pending publication", err)
	}
	if delayed.loaded[state.Active.ID].Artifact.Digest != previous.Artifact.Digest || !delayed.loaded[state.Active.ID].Artifact.Configuration.ValidUntil.After(runtime.now) {
		test.Fatal("previous serving authorization is no longer valid")
	}
	runtime.now = runtime.now.Add(31 * time.Second)
	if err = reconciler.Reconcile(ctx, tenantID, appID); err != nil {
		test.Fatal("must recover after projection", err)
	}
	state, err = stateStore.GetManagedSSHControl(tenantID, appID)
	if err != nil || !state.Active.Verified(runtime.now) || state.Active.PendingApply != nil || state.Active.LoadedArtifact.Digest != desired || state.Active.LastValidEvidence.ArtifactDigest != desired {
		test.Fatal("did not converge", err)
	}
}

func TestProjectionDeadlinePersistsAndDoesNotChurnDesired(test *testing.T) {
	ctx := context.Background()
	r, runtime, tenantID, appID := projectionFixture(test)
	runtime.delay = 10 * time.Minute
	runtime.now = runtime.now.Add(80 * time.Second)
	if err := r.Reconcile(ctx, tenantID, appID); err != nil {
		test.Fatal(err)
	}
	state, _ := r.Store.GetManagedSSHControl(tenantID, appID)
	desired, deadline := state.Active.Artifact.Digest, *state.Active.PendingApply
	runtime.now = runtime.now.Add(80 * time.Second)
	if err := r.Reconcile(ctx, tenantID, appID); err != nil {
		test.Fatal("old artifact inside deadline rejected", err)
	}
	r = &Reconciler{Store: runtime.restartStore(), Runtime: runtime, Namespace: r.Namespace, SigningKeyFile: r.SigningKeyFile, Now: r.Now}
	runtime.store = r.Store
	runtime.now = deadline.Deadline
	err := r.Reconcile(ctx, tenantID, appID)
	if err == nil || !strings.Contains(err.Error(), "apply_deadline_exceeded") {
		test.Fatal("propagation timeout hidden", err)
	}
	state, _ = r.Store.GetManagedSSHControl(tenantID, appID)
	if state.Active.Artifact.Digest != desired || *state.Active.PendingApply != deadline || state.Active.Serving(runtime.now) || state.Active.LastValidEvidence == nil {
		test.Fatal("deadline reset or expired pending served")
	}
	// Exact confirmation recovers without changing the target or deadline.
	runtime.loaded[state.Active.ID] = runtime.pending[state.Active.ID]
	if err := r.Reconcile(ctx, tenantID, appID); err != nil {
		test.Fatal(err)
	}
	state, _ = r.Store.GetManagedSSHControl(tenantID, appID)
	if !state.Active.Verified(runtime.now) || state.Active.PendingApply != nil {
		test.Fatal("late exact confirmation failed")
	}
}

func TestProjectionRevocationAndUnknownDigestPreserveHistoryButRejectServing(test *testing.T) {
	for _, scenario := range []string{"revoke", "unknown", "expired"} {
		test.Run(scenario, func(test *testing.T) {
			ctx := context.Background()
			r, runtime, tenantID, appID := projectionFixture(test)
			runtime.delay = 10 * time.Minute
			runtime.now = runtime.now.Add(80 * time.Second)
			if err := r.Reconcile(ctx, tenantID, appID); err != nil {
				test.Fatal(err)
			}
			state, _ := r.Store.GetManagedSSHControl(tenantID, appID)
			previousPositive := *state.Active.LastValidEvidence
			switch scenario {
			case "revoke":
				key := state.Active.Artifact.Configuration.Keys[0].ID
				if _, err := r.Store.DeleteSSHKey(key); err != nil {
					test.Fatal(err)
				}
			case "unknown":
				loaded := runtime.loaded[state.Active.ID]
				loaded.Artifact.Digest = "sha256:" + strings.Repeat("c", 64)
				runtime.loaded[state.Active.ID] = loaded
			case "expired":
				runtime.now = runtime.loaded[state.Active.ID].Artifact.Configuration.ValidUntil
			}
			err := r.Reconcile(ctx, tenantID, appID)
			if err == nil {
				test.Fatal("unsafe observation accepted")
			}
			state, _ = r.Store.GetManagedSSHControl(tenantID, appID)
			if state.Active.Verified(runtime.now) || state.Active.Serving(runtime.now) || *state.Active.LastValidEvidence != previousPositive {
				test.Fatal("rejection either lost history or continued serving")
			}
			if scenario == "revoke" && len(runtime.published[len(runtime.published)-1].Artifact.Configuration.Keys) != 0 {
				test.Fatal("revocation not published")
			}
		})
	}
}

func TestProjectionMultipleSlotsProgressIndependently(test *testing.T) {
	ctx := context.Background()
	r, runtime, tenantID, appID := projectionFixture(test)
	runtime.target.PodUID, runtime.target.ContainerID = "second-pod", "second-container"
	if err := r.Reconcile(ctx, tenantID, appID); err != nil {
		test.Fatal(err)
	}
	state, _ := r.Store.GetManagedSSHControl(tenantID, appID)
	active, err := state.ActivateCandidate(state.Candidate.Artifact.Digest, state.Candidate.Artifact.Configuration.Identity.Fingerprint, state.LegacyRouteDigest, "operator", runtime.now)
	if err != nil {
		test.Fatal(err)
	}
	if err := r.Store.SaveManagedSSHControl(state.Revision, active); err != nil {
		test.Fatal(err)
	}
	runtime.target.PodUID, runtime.target.ContainerID = "third-pod", "third-container"
	if err := r.Reconcile(ctx, tenantID, appID); err != nil {
		test.Fatal(err)
	}
	runtime.now = runtime.now.Add(80 * time.Second)
	if err := r.Reconcile(ctx, tenantID, appID); err != nil {
		test.Fatal(err)
	}
	state, _ = r.Store.GetManagedSSHControl(tenantID, appID)
	if len(state.Slots()) != 3 {
		test.Fatal("fixture requires active, candidate and draining")
	}
	for _, slot := range state.Slots() {
		if slot.Verified(runtime.now) || !slot.Serving(runtime.now) || slot.PendingApply == nil {
			test.Fatal("slot did not wait independently", slot.ID)
		}
	}
	// One corrupt slot must not prevent the others from acknowledging.
	runtime.now = runtime.now.Add(31 * time.Second)
	badID := state.Draining[0].ID
	bad := runtime.pending[badID]
	bad.Artifact.Digest = "sha256:" + strings.Repeat("d", 64)
	runtime.loaded[badID] = bad
	runtime.readyAt[badID] = runtime.now.Add(time.Hour)
	if err := r.Reconcile(ctx, tenantID, appID); err == nil {
		test.Fatal("unknown digest accepted")
	}
	state, _ = r.Store.GetManagedSSHControl(tenantID, appID)
	if !state.Active.Verified(runtime.now) || !state.Candidate.Verified(runtime.now) || state.Draining[0].Serving(runtime.now) {
		test.Fatal("failed slot prevented independent convergence")
	}
}

func TestProjectionRestartAfterAuthorizationExpiryCanRecoverExactly(test *testing.T) {
	ctx := context.Background()
	r, runtime, tenantID, appID := projectionFixture(test)
	runtime.delay = time.Hour
	runtime.now = runtime.now.Add(80 * time.Second)
	if err := r.Reconcile(ctx, tenantID, appID); err != nil {
		test.Fatal(err)
	}
	state, _ := r.Store.GetManagedSSHControl(tenantID, appID)
	deadline, oldDesired := *state.Active.PendingApply, state.Active.Artifact.Digest
	runtime.now = state.Active.Artifact.Configuration.ValidUntil.Add(time.Second)
	r = &Reconciler{Store: runtime.restartStore(), Runtime: runtime, Namespace: r.Namespace, SigningKeyFile: r.SigningKeyFile, Now: r.Now}
	runtime.store = r.Store
	if err := r.Reconcile(ctx, tenantID, appID); err == nil {
		test.Fatal("expired old authorization accepted")
	}
	state, _ = r.Store.GetManagedSSHControl(tenantID, appID)
	if state.Active.Artifact.Digest == oldDesired || *state.Active.PendingApply != deadline || state.Active.Serving(runtime.now) {
		test.Fatal("expired desired was not renewed with original deadline")
	}
	runtime.loaded[state.Active.ID] = runtime.pending[state.Active.ID]
	if err := r.Reconcile(ctx, tenantID, appID); err != nil {
		test.Fatal("fresh exact observation cannot recover after restart", err)
	}
	state, _ = r.Store.GetManagedSSHControl(tenantID, appID)
	if !state.Active.Verified(runtime.now) || state.Active.PendingApply != nil {
		test.Fatal("renewed artifact did not converge")
	}
}
