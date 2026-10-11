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

func verifyManagedSSHRouteProjection(test *testing.T, stateStore *Store, owner model.App) {
	test.Helper()
	app, err := stateStore.CreateApp(owner.TenantID, owner.ProjectID, model.NewID("managed-route"), "", model.AppSpec{Image: "registry.example/unchanged:v1", Replicas: 1})
	if err != nil {
		test.Fatal(err)
	}
	identity, err := stateStore.PrepareAppSSHIdentity(app.TenantID, app.ID)
	if err != nil {
		test.Fatal(err)
	}
	defer stateStore.db.Exec(`DELETE FROM fugue_managed_ssh_routes WHERE app_id=$1`, app.ID)
	defer stateStore.db.Exec(`DELETE FROM fugue_managed_ssh_route_history WHERE app_id=$1`, app.ID)
	defer stateStore.db.Exec(`DELETE FROM fugue_managed_ssh_artifacts WHERE app_id=$1`, app.ID)
	defer stateStore.db.Exec(`DELETE FROM fugue_managed_ssh_control WHERE app_id=$1`, app.ID)
	key, err := stateStore.CreateSSHKey(app.TenantID, "route-user", identity.PublicKey)
	if err != nil {
		test.Fatal(err)
	}
	app, legacy, err := stateStore.UpsertAppSSHConfig(app.ID, AppSSHUpdate{Enabled: true, Hostname: "ssh.example", PublicPortStart: 28000, PublicPortEnd: 29000, AuthorizedKeyIDs: []string{key.ID}})
	if err != nil {
		test.Fatal(err)
	}
	spec := managedSSHSpec(identity.ID)
	spec.AuthorizedKeyIDs = []string{key.ID}
	intent, err := stateStore.PutManagedSSHIntent(app.TenantID, app.ID, 0, spec)
	if err != nil {
		test.Fatal(err)
	}
	binding, err := stateStore.PrepareManagedSSHBinding(app.TenantID, app.ID, identity.ID, model.AppSSHRouteDigest(legacy), "ssh.example", 28000, 29000)
	if err != nil {
		test.Fatal(err)
	}
	now := time.Now().UTC()
	target := managedssh.Target{TenantID: app.TenantID, AppID: app.ID, Namespace: "workloads", PodName: "workspace", PodUID: "pod-uid", ContainerID: "container-id", Container: spec.Container, User: spec.User, WorkingDirectory: spec.WorkingDirectory}
	configuration, err := managedssh.CompileConfiguration(intent, identity, target, []model.SSHKey{key}, 1, now)
	if err != nil {
		test.Fatal(err)
	}
	signingKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{97}, 32))
	artifact, err := managedssh.SignArtifact(configuration, "config-key", signingKey)
	if err != nil {
		test.Fatal(err)
	}
	release := managedssh.RuntimeRelease{Image: "registry.example/ssh@sha256:" + strings.Repeat("a", 64), HelperDigest: "sha256:" + strings.Repeat("b", 64), Architecture: "amd64"}
	slot := &managedssh.ServingSlot{ID: managedssh.SlotID(identity.ID, target, release), Namespace: "platform-ssh", Release: release, Artifact: artifact, CreatedAt: now}
	state := managedssh.ControlState{Schema: "fugue.managed-ssh-control/v1", TenantID: app.TenantID, AppID: app.ID, IdentityID: identity.ID, Revision: 1, Generation: 1, IntentGeneration: intent.Generation, Candidate: slot}
	if err := stateStore.SaveManagedSSHControl(0, state); err != nil {
		test.Fatal(err)
	}
	activate := func(revision uint64) (managedssh.ControlState, error) {
		return stateStore.ActivateManagedSSH(app.TenantID, app.ID, revision, intent.Generation, state.Candidate.Artifact.Digest, identity.Fingerprint, binding.ID, binding.LegacyRouteDigest, "reviewer")
	}
	if _, err := activate(1); !errors.Is(err, managedssh.ErrCandidateUnverified) {
		test.Fatal("unverified candidate was published", err)
	}
	state.Candidate.Evidence = &managedssh.CandidateEvidence{ArtifactDigest: artifact.Digest, IdentityID: identity.ID, Fingerprint: identity.Fingerprint, ServerPodUID: "serving-pod", TargetPodUID: target.PodUID, TargetContainerID: target.ContainerID, KeyExchangeVerified: true, RuntimeVerified: true, ObservedAt: now, ExpiresAt: now.Add(time.Minute)}
	state.Revision++
	if err := stateStore.SaveManagedSSHControl(1, state); err != nil {
		test.Fatal(err)
	}
	if _, err := activate(1); !errors.Is(err, ErrConflict) {
		test.Fatal("stale activation precondition accepted", err)
	}
	if _, err := stateStore.db.Exec(`UPDATE fugue_ssh_keys SET status='revoked' WHERE id=$1`, key.ID); err != nil {
		test.Fatal(err)
	}
	if _, err := activate(2); !errors.Is(err, ErrConflict) {
		test.Fatal("deleted authorization resurrected during activation", err)
	}
	if _, err := stateStore.db.Exec(`UPDATE fugue_ssh_keys SET status='active' WHERE id=$1`, key.ID); err != nil {
		test.Fatal(err)
	}
	if _, err := stateStore.db.Exec(`UPDATE fugue_app_ssh_endpoints SET target_host='other.example' WHERE app_id=$1`, app.ID); err != nil {
		test.Fatal(err)
	}
	if _, err := activate(2); !errors.Is(err, ErrConflict) {
		test.Fatal("changed legacy target passed reviewed route precondition", err)
	}
	if _, err := stateStore.db.Exec(`UPDATE fugue_app_ssh_endpoints SET target_host=$1 WHERE app_id=$2`, legacy.TargetHost, app.ID); err != nil {
		test.Fatal(err)
	}
	activated, err := activate(2)
	if err != nil {
		test.Fatal(err)
	}
	endpoint, err := stateStore.GetAppSSHEndpoint(app.ID)
	if err != nil {
		test.Fatal(err)
	}
	if endpoint.TargetService != slot.ID || endpoint.TargetPort != 2222 || endpoint.HostKeyFingerprint != identity.Fingerprint || endpoint.PublicPort != legacy.PublicPort || activated.RouteGeneration != 1 {
		test.Fatal("activation did not atomically publish the managed target", endpoint)
	}
	after, err := stateStore.GetApp(app.ID)
	if err != nil || !reflect.DeepEqual(app.Spec, after.Spec) {
		test.Fatal("activation edited application workload", err)
	}
	if _, err := stateStore.db.Exec(`UPDATE fugue_app_ssh_endpoints SET target_host='image-backed.example',target_port=22,host_key_fingerprint='',status='disabled',released_at=now() WHERE app_id=$1`, app.ID); err != nil {
		test.Fatal(err)
	}
	projected, err := scanAppSSHEndpoint(stateStore.db.QueryRow(appSSHEndpointSelectColumns+` WHERE app_id=$1`, app.ID))
	if err != nil || projected.TargetHost != endpoint.TargetHost || projected.TargetPort != 2222 || projected.Status != model.AppSSHEndpointStatusPending || projected.ReleasedAt != nil || projected.HostKeyFingerprint != identity.Fingerprint {
		test.Fatal("older application writer replaced the managed route", projected, err)
	}
	if _, err := stateStore.db.Exec(`DELETE FROM fugue_app_ssh_endpoints WHERE app_id=$1`, app.ID); err != nil {
		test.Fatal(err)
	}
	if _, err := stateStore.GetAppSSHEndpoint(app.ID); err != nil {
		test.Fatal("legacy cleanup erased managed serving projection", err)
	}
	routes, err := stateStore.ListEdgeSSHRoutes(AppSSHRouteOptions{})
	if err != nil {
		test.Fatal(err)
	}
	found := false
	for _, route := range routes {
		if route.AppID == app.ID {
			found = route.TargetHost == endpoint.TargetHost && route.TargetPort == 2222
		}
	}
	if !found {
		test.Fatal("legacy edge bundle producer lost activated SSH route")
	}
	if _, err := activate(2); !errors.Is(err, ErrConflict) {
		test.Fatal("replayed activation accepted", err)
	}
	secondRelease := release
	secondRelease.Image = "registry.example/ssh@sha256:" + strings.Repeat("c", 64)
	configuration.Generation = 2
	secondArtifact, err := managedssh.SignArtifact(configuration, "config-key", signingKey)
	if err != nil {
		test.Fatal(err)
	}
	second := *slot
	second.ID, second.Release, second.Artifact = managedssh.SlotID(identity.ID, target, secondRelease), secondRelease, secondArtifact
	secondEvidence := *slot.Evidence
	secondEvidence.ArtifactDigest, secondEvidence.ServerPodUID = secondArtifact.Digest, "second-server-pod"
	second.Evidence = &secondEvidence
	staged := activated
	staged.Revision++
	staged.Generation = 2
	staged.Candidate = &second
	if err := stateStore.SaveManagedSSHControl(activated.Revision, staged); err != nil {
		test.Fatal(err)
	}
	secondActive, err := stateStore.ActivateManagedSSH(app.TenantID, app.ID, staged.Revision, intent.Generation, secondArtifact.Digest, identity.Fingerprint, binding.ID, binding.LegacyRouteDigest, "reviewer")
	if err != nil {
		test.Fatal(err)
	}
	if len(secondActive.Draining) != 1 || secondActive.Active.ID != second.ID {
		test.Fatal("previous slot was not retained for draining")
	}
	if _, err := stateStore.db.Exec(`UPDATE fugue_ssh_keys SET status='revoked' WHERE id=$1`, key.ID); err != nil {
		test.Fatal(err)
	}
	if _, err := stateStore.RollbackManagedSSH(app.TenantID, app.ID, secondActive.Revision, intent.Generation, artifact.Digest, identity.Fingerprint, binding.ID, binding.LegacyRouteDigest, "reviewer", slot.ID); !errors.Is(err, ErrConflict) {
		test.Fatal("rollback restored revoked grants", err)
	}
	if _, err := stateStore.db.Exec(`UPDATE fugue_ssh_keys SET status='active' WHERE id=$1`, key.ID); err != nil {
		test.Fatal(err)
	}
	rolledBack, err := stateStore.RollbackManagedSSH(app.TenantID, app.ID, secondActive.Revision, intent.Generation, artifact.Digest, identity.Fingerprint, binding.ID, binding.LegacyRouteDigest, "reviewer", slot.ID)
	if err != nil {
		test.Fatal(err)
	}
	if rolledBack.Active.ID != slot.ID || !rolledBack.RollbackHold || rolledBack.RouteGeneration != 3 {
		test.Fatal("rollback did not retain identity and hold automatic promotion")
	}
	rolledBackEndpoint, err := stateStore.GetAppSSHEndpoint(app.ID)
	if err != nil || rolledBackEndpoint.TargetHost != endpoint.TargetHost || rolledBackEndpoint.HostKeyFingerprint != identity.Fingerprint {
		test.Fatal("rollback changed host identity or missed old verified route", err)
	}
	var historyCount int
	if err := stateStore.db.QueryRow(`SELECT count(*) FROM fugue_managed_ssh_route_history WHERE app_id=$1 AND actor='reviewer'`, app.ID).Scan(&historyCount); err != nil || historyCount != 3 {
		test.Fatal("route history omitted an activation or rollback", historyCount, err)
	}
	retiring := rolledBack
	retiring.Draining = append([]managedssh.ServingSlot(nil), rolledBack.Draining...)
	retiringSlot := &retiring.Draining[0]
	retiringSlot.RetiringAt = &now
	retiringSlot.Artifact.Configuration.Draining = true
	retiringSlot.Artifact.Configuration.Generation = retiring.Generation + 1
	retiringSlot.Artifact, err = managedssh.SignArtifact(retiringSlot.Artifact.Configuration, "config-key", signingKey)
	if err != nil {
		test.Fatal(err)
	}
	retiringSlot.Evidence = nil
	retiring.Generation++
	retiring.Revision++
	if err := stateStore.SaveManagedSSHControl(rolledBack.Revision, retiring); err != nil {
		test.Fatal(err)
	}
	if _, err := stateStore.RollbackManagedSSH(app.TenantID, app.ID, retiring.Revision, intent.Generation, retiringSlot.Artifact.Digest, identity.Fingerprint, binding.ID, binding.LegacyRouteDigest, "reviewer", retiringSlot.ID); err == nil {
		test.Fatal("retiring slot was eligible for rollback")
	}
	dropped := retiring
	dropped.Draining = nil
	dropped.Revision++
	if err := stateStore.SaveManagedSSHControl(retiring.Revision, dropped); !errors.Is(err, ErrConflict) {
		test.Fatal("retirement silently dropped retained metadata", err)
	}
	retiredSlot := *retiringSlot
	retiredSlot.RetiredAt = &now
	dropped.Retired = append(dropped.Retired, retiredSlot)
	if err := stateStore.SaveManagedSSHControl(retiring.Revision, dropped); err != nil {
		test.Fatal(err)
	}
	restored, err := stateStore.GetManagedSSHControl(app.TenantID, app.ID)
	if err != nil || len(restored.Retired) != 1 || len(restored.Draining) != 0 || restored.Active.ID != slot.ID {
		test.Fatal("retirement changed serving state or lost recovery history", err)
	}
}
