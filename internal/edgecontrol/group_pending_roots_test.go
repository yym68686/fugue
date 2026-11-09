package edgecontrol

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/inputarchive"
	"fugue/internal/model"
	"fugue/internal/trafficbinding"
)

type pendingRegistrarFunc func(context.Context, inputarchive.PendingRootRegistration) (inputarchive.PendingRootReceipt, error)

func (f pendingRegistrarFunc) RetainPendingCompilerInputExternalRoot(ctx context.Context, r inputarchive.PendingRootRequest) (inputarchive.PendingRootReceipt, error) {
	return f(ctx, r.Registration)
}

func pendingPublicationFixture(t *testing.T, bound bool) (*PersistentGroupStore, *fixtureGroupSigner, string, time.Time, GroupShadowBatch) {
	t.Helper()
	s, group, now, intent := compileInputFixture(t, true)
	if bound {
		digest := "sha256:" + string(bytes.Repeat([]byte{'a'}, 64))
		intent.TrafficRelease = &model.TrafficReleaseBinding{Schema: trafficbinding.Schema, ReleaseSetID: "release-set", ReleaseSetDigest: digest, ReleaseSetGeneration: "parent-generation",
			RouteArtifactID: "route-artifact", RouteArtifactDigest: digest, RouteArtifactGeneration: intent.Generation, RouteArtifactSequence: 1, ReleaseID: "release-full", ReleaseChannel: "full",
			FencingToken: 1, ScopeKey: "global", IntentDigest: digest, PolicyDigest: digest, InputSnapshotDigest: digest, CompilerVersion: "compiler", ProjectionDigest: trafficbinding.ProjectionDigest(intent)}
	}
	batch := compileInputRun(t, s, group, now, intent)
	if batch.Succeeded != 1 {
		t.Fatal(batch)
	}
	return s, &fixtureGroupSigner{keys: map[string][]byte{group: bytes.Repeat([]byte{0x73}, 32)}, validFor: time.Hour}, group, now, batch
}

func exactPendingReceipt(r inputarchive.PendingRootRegistration) inputarchive.PendingRootReceipt {
	_, hash, _ := r.Canonical()
	return inputarchive.PendingRootReceipt{RegistrationSHA256: hash, RegistryRevision: 1, SourceEpoch: 0}
}

func reopenPendingPublication(t *testing.T, s *PersistentGroupStore, registrar GroupPendingRootRegistrar) *PersistentGroupStore {
	t.Helper()
	next, err := OpenPersistentGroupStoreWithOptions(s.root, PersistentGroupStoreOptions{RetainCompileInputs: true, CompileInputKeyringDir: s.compileInputKeyringDir, PendingRootRegistrar: registrar})
	if err != nil {
		t.Fatal(err)
	}
	return next
}

func TestGroupPendingRootRegistersBeforeFirstPublicationAndReusesAcrossRefresh(t *testing.T) {
	s, signer, group, now, batch := pendingPublicationFixture(t, true)
	calls := 0
	var registration inputarchive.PendingRootRegistration
	registrar := pendingRegistrarFunc(func(ctx context.Context, r inputarchive.PendingRootRegistration) (inputarchive.PendingRootReceipt, error) {
		calls++
		registration = r
		state, err := s.readGroupState(s.groupStatePath(group), group)
		if err != nil || state.Published != nil || state.PendingRootsRequired {
			t.Fatal("local publication preceded durable registration", err)
		}
		if _, _, err := r.Canonical(); err != nil || r.Owner != "edge-control/"+group {
			t.Fatal(r, err)
		}
		return exactPendingReceipt(r), nil
	})
	s = reopenPendingPublication(t, s, registrar)
	publisher := GroupAuthorityPublisher{Store: s, Signer: signer, Now: func() time.Time { return now }}
	result, err := publisher.Publish(t.Context(), batch)
	if err != nil || result.Published != 1 || calls != 1 {
		t.Fatal(result, calls, err)
	}
	state, err := s.readGroupState(s.groupStatePath(group), group)
	if err != nil || !state.PendingRootsRequired || len(state.PendingRootRecords) != 1 || !state.PendingRootRecords[0].Receipt.Matches(registration) {
		t.Fatal("local CAS omitted exact registration receipt", err)
	}
	copy := clonePersistentGroupState(state)
	copy.PendingRootRecords[0].Receipt.RegistryRevision++
	if copy.PendingRootRecords[0].Receipt == state.PendingRootRecords[0].Receipt {
		t.Fatal("receipt clone aliases durable cache")
	}
	resultOne, err := publisher.RefreshPublishedLKG(t.Context(), group, now.Add(time.Hour))
	if err != nil || resultOne.Status != GroupAuthorityStatusPublished || calls != 1 {
		t.Fatal("LKG refresh duplicated source root", resultOne, calls, err)
	}
	// Every positive path passes through the native file CAS. Reopening with
	// the option off cannot bypass the persisted requirement; failed audits
	// continue to append and the existing LKG remains independently readable.
	without, err := OpenPersistentGroupStore(s.root)
	if err != nil {
		t.Fatal(err)
	}
	before, err := without.ReadGroupAuthority(t.Context(), group)
	if err != nil {
		t.Fatal(err)
	}
	failed, err := (GroupAuthorityPublisher{Store: without, Signer: signer}).RefreshPublishedLKG(t.Context(), group, now.Add(2*time.Hour))
	after, readErr := without.ReadGroupAuthority(t.Context(), group)
	if err != nil || readErr != nil || failed.Status != GroupAuthorityStatusFailed || !sameGroupCompileValue(before.Published, after.Published) || after.LedgerHead.Sequence <= before.LedgerHead.Sequence {
		t.Fatal("restart downgraded policy or lost LKG/audit", failed, err, readErr)
	}
	again, err := (GroupAuthorityPublisher{Store: without, Signer: signer}).RefreshPublishedLKG(t.Context(), group, now.Add(3*time.Hour))
	if err != nil || again.PublicationSequence != after.LedgerHead.Sequence || again.FailureCode != GroupAuthorityFailurePendingRetention {
		t.Fatal("unchanged retention failure flooded mandatory audit", again, err)
	}
	// The reader-first export still handles the new optional bounded format;
	// it does not turn a retention receipt into authenticated full inventory.
	out, err := ExportGroupOriginalEvidence(t.Context(), s.groupStatePath(group), group, s.compileInputKeyringDir, []string{state.Published.Bundle.Generation})
	if err != nil || out.AdmissionAuthenticated || out.DependencyClosure {
		t.Fatal(out, err)
	}
}

func TestGroupPendingRootLostReplyRetainsOrphanAndExactRetry(t *testing.T) {
	s, signer, group, now, batch := pendingPublicationFixture(t, true)
	remote := map[string]inputarchive.PendingRootReceipt{}
	calls := 0
	registrar := pendingRegistrarFunc(func(ctx context.Context, r inputarchive.PendingRootRegistration) (inputarchive.PendingRootReceipt, error) {
		calls++
		receipt := exactPendingReceipt(r)
		remote[receipt.RegistrationSHA256] = receipt
		if calls == 1 {
			return inputarchive.PendingRootReceipt{}, context.DeadlineExceeded
		}
		return receipt, nil
	})
	s = reopenPendingPublication(t, s, registrar)
	publisher := GroupAuthorityPublisher{Store: s, Signer: signer, Now: func() time.Time { return now }}
	first, err := publisher.Publish(t.Context(), batch)
	state, readErr := s.readGroupState(s.groupStatePath(group), group)
	if err != nil || readErr != nil || first.Failed != 1 || state.Published != nil || state.PendingRootsRequired || len(remote) != 1 || len(state.AuthorityLedger) != 1 {
		t.Fatal("lost reply published or discarded remote root/audit", first, err, readErr)
	}
	second, err := publisher.Publish(t.Context(), batch)
	if err != nil || second.Published != 1 || calls != 2 || len(remote) != 1 {
		t.Fatal("exact source retry replaced or duplicated remote protection", second, calls, len(remote), err)
	}
}

func TestGroupPendingRootRefusesMissingOriginalBadReceiptAndPolicyDrift(t *testing.T) {
	for _, mode := range []string{"unbound", "wrong-receipt", "revoked-during-registration"} {
		t.Run(mode, func(t *testing.T) {
			s, signer, group, now, batch := pendingPublicationFixture(t, mode != "unbound")
			commits := 0
			registrar := pendingRegistrarFunc(func(ctx context.Context, r inputarchive.PendingRootRegistration) (inputarchive.PendingRootReceipt, error) {
				commits++
				receipt := exactPendingReceipt(r)
				if mode == "wrong-receipt" {
					receipt.RegistrationSHA256 = "unbound"
				}
				if mode == "revoked-during-registration" {
					path := filepath.Join(s.compileInputKeyringDir, group+".json")
					keys, err := loadGroupBundleSigningKeyring(path, group)
					if err != nil {
						t.Fatal(err)
					}
					keys.RevokedKeyIDs = append(keys.RevokedKeyIDs, keys.PrimaryKeyID)
					raw, err := json.Marshal(keys)
					if err != nil || os.WriteFile(path, raw, 0o600) != nil {
						t.Fatal("test policy change failed", err)
					}
				}
				return receipt, nil
			})
			s = reopenPendingPublication(t, s, registrar)
			result, err := (GroupAuthorityPublisher{Store: s, Signer: signer, Now: func() time.Time { return now }}).Publish(t.Context(), batch)
			state, readErr := s.readGroupState(s.groupStatePath(group), group)
			if err != nil || readErr != nil || result.Failed != 1 || state.Published != nil || len(state.PendingRootRecords) != 0 || len(state.AuthorityLedger) != 1 || mode == "unbound" && commits != 0 {
				t.Fatal("unadmitted source changed serving authority", result, commits, err, readErr)
			}
		})
	}
}

func TestGroupPendingRootTypedReferencesUseNativeSHAContract(t *testing.T) {
	s, _, group, keyDir, _ := publicationInputFixture(t, true)
	state, err := s.readGroupState(s.groupStatePath(group), group)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := loadGroupBundleSigningKeyring(filepath.Join(keyDir, group+".json"), group)
	if err != nil {
		t.Fatal(err)
	}
	b, err := bindGroupOriginalPublication(group, state.RetentionInputs[0], state.AuthorityLedger[0], state.Published.Bundle, 0, keys)
	if err != nil {
		t.Fatal(err)
	}
	r, err := pendingGroupRootRegistration(group, b)
	if err != nil {
		t.Fatal("export references were not usable native roots", err)
	}
	roots := []inputarchive.HotRoot{}
	for _, ref := range r.References {
		roots = append(roots, inputarchive.HotRoot{Source: "test", ID: ref.ID, Reference: ref})
	}
	_, err = inputarchive.PlanHotClosure(t.Context(), roots, inputarchive.HotClosureBudget{Roots: 32, Nodes: 32, Edges: 32, Bindings: 64}, func(context.Context, inputarchive.HotReference) (inputarchive.HotNode, error) {
		return inputarchive.HotNode{}, errors.New("stop after native identity validation")
	})
	if inputarchive.HotClosureFailureReason(err) != "resolver-unavailable" {
		t.Fatal("typed SHA references refused before native resolution", err)
	}
}
