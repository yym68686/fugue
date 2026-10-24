package store

import (
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

// Runs after the real signed DNS gray/full/capability/LKG fixture, in both
// stores. No fabricated ACK or replacement LKG is used to exercise this CAS.
func testDNSPublicationSuccessorGuard(t *testing.T, s *Store, input platformconfig.CompileRequest, old model.PlatformArtifact, shadow, gray, full model.PlatformArtifactRelease) {
	t.Helper()
	lkg, err := s.GetPlatformLKG(old.ArtifactKind, old.ScopeKey)
	if err != nil || lkg == nil {
		t.Fatal("missing real DNS LKG", err)
	}
	ref := func(a model.PlatformArtifact, r model.PlatformArtifactRelease) *model.PlatformPublicationPrecondition {
		return &model.PlatformPublicationPrecondition{ArtifactID: a.ID, ContentHash: a.ContentHash, ReleaseID: r.ID, FencingToken: r.FencingToken}
	}
	base := model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow", DNSPublicationPrecondition: &model.PlatformDNSPublicationPrecondition{
		Lanes: map[string]*model.PlatformPublicationPrecondition{"shadow": ref(old, shadow), "gray": ref(old, gray), "full": ref(old, full)},
		LKGID: lkg.ID, LKGPublication: *ref(old, full), VerificationEvidenceHash: lkg.VerificationEvidenceHash,
	}}
	save := func(a model.PlatformArtifact) model.PlatformArtifact {
		t.Helper()
		a, err := s.CreatePlatformArtifact(a)
		if err == nil {
			a, err = s.ValidatePlatformArtifact(a.ID, []model.PlatformArtifactValidationResult{{Name: "compiler", Pass: true}})
		}
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	candidate := func(name string) model.PlatformArtifact {
		t.Helper()
		in := input
		in.Intent.Generation += "-" + name
		in.Policy.Generation += "-" + name
		in.RuntimeSnapshot.IntentGeneration, in.RuntimeSnapshot.PolicyGeneration = "", ""
		compiled, err := platformconfig.Compile(in)
		if err != nil {
			t.Fatal(err)
		}
		save(compiled.PolicyArtifact)
		child := save(compiled.DNSArtifact)
		return save(platformconfig.BuildReleaseSetArtifact(compiled.ReleaseSet, []string{child.ID}, time.Now().UTC()))
	}
	next := candidate("successor")
	request := func(a model.PlatformArtifact) model.PlatformArtifactReleaseRequest {
		t.Helper()
		raw, _ := json.Marshal(base)
		var req model.PlatformArtifactReleaseRequest
		if err := json.Unmarshal(raw, &req); err != nil {
			t.Fatal(err)
		}
		req.IdempotencyKey, err = dnsPublicationKey(a, req)
		if err != nil {
			t.Fatal(err)
		}
		return req
	}
	preserve := func() {
		t.Helper()
		current, err := s.GetPlatformLKG(old.ArtifactKind, old.ScopeKey)
		_, selected, found, readErr := s.GetActivePlatformArtifact(old.ArtifactKind, old.ScopeKey, "full")
		if err != nil || !reflect.DeepEqual(lkg, current) || readErr != nil || !found || selected.ID != full.ID {
			t.Fatal("shadow update changed serving full or positive LKG", err, readErr)
		}
	}
	for _, scenario := range []string{"shadow", "gray", "full", "absent-gray", "missing-lane", "lkg-id", "lkg-fence", "evidence", "override", "foreign-kind", "wrong-key", "generation-alias"} {
		t.Run("successor rejects "+scenario, func(t *testing.T) {
			req := request(next)
			a := next
			switch scenario {
			case "shadow", "gray", "full":
				req.DNSPublicationPrecondition.Lanes[scenario].FencingToken++
			case "absent-gray":
				req.DNSPublicationPrecondition.Lanes["gray"] = nil
			case "missing-lane":
				delete(req.DNSPublicationPrecondition.Lanes, "gray")
			case "lkg-id":
				req.DNSPublicationPrecondition.LKGID = "foreign"
			case "lkg-fence":
				req.DNSPublicationPrecondition.LKGPublication.FencingToken++
			case "evidence":
				req.DNSPublicationPrecondition.VerificationEvidenceHash = old.ContentHash
			case "override":
				req.SoftOverride = true
			case "foreign-kind":
				if len(input.CellRoutePublications) == 0 {
					a = input.PreviousTrafficPublication.Parent
				} else {
					a = input.CellRoutePublications[0].Parent
				}
			}
			req.IdempotencyKey, _ = dnsPublicationKey(a, req)
			if scenario == "wrong-key" {
				req.IdempotencyKey = "unbound"
			}
			id := a.ID
			if scenario == "generation-alias" {
				id = a.Generation
			}
			if _, _, _, _, err := s.ReleasePlatformArtifact(id, req, testPlatformPrincipal()); err == nil {
				t.Fatal("invalid DNS precondition wrote a publication")
			}
			preserve()
		})
	}
	if s.db == nil {
		var frozen model.State
		if err := s.withLockedState(false, func(state *model.State) error {
			raw, err := json.Marshal(state)
			if err != nil {
				return err
			}
			return json.Unmarshal(raw, &frozen)
		}); err != nil {
			t.Fatal(err)
		}
		req := request(next)
		if err := validateDNSPublicationGuard(&frozen, next, req, s.platformArtifactSigningKeyring(), time.Now().UTC()); err != nil {
			t.Fatal("valid snapshot rejected", err)
		}
		if err := validateDNSPublicationGuard(&frozen, next, req, s.platformArtifactSigningKeyring(), lkg.ExpiresAt); !errors.Is(err, ErrConflict) {
			t.Fatal("expired positive LKG admitted successor", err)
		}
		for i := range frozen.PlatformReleaseLanes {
			if frozen.PlatformReleaseLanes[i].ScopeKey != old.ScopeKey {
				continue
			}
			frozen.PlatformReleaseLanes[i].Frozen = true
			if err := validateDNSPublicationGuard(&frozen, next, req, s.platformArtifactSigningKeyring(), time.Now().UTC()); !errors.Is(err, ErrConflict) {
				t.Fatal("frozen DNS lane admitted successor", err)
			}
			frozen.PlatformReleaseLanes[i].Frozen = false
		}
	}
	for _, channel := range []string{"gray", "full"} {
		req := request(next)
		req.ReleaseChannel = channel
		if channel == "gray" {
			req.CanaryRuleRef = "cohort=complete"
		}
		req.IdempotencyKey, _ = dnsPublicationKey(next, req)
		if _, _, _, _, err := s.ReleasePlatformArtifact(next.ID, req, testPlatformPrincipal()); !errors.Is(err, ErrConflict) {
			t.Fatal("CAS guard bypassed successor capability or convergence gates", channel, err)
		}
		preserve()
	}
	req := request(next)
	_, successor, _, _, err := s.ReleasePlatformArtifact(next.ID, req, testPlatformPrincipal())
	if err != nil {
		t.Fatal("valid successor rejected", err)
	}
	_, retry, _, _, err := s.ReleasePlatformArtifact(next.ID, req, testPlatformPrincipal())
	if err != nil || retry.ID != successor.ID {
		t.Fatal("current exact retry not idempotent", err)
	}
	preserve()
	base.DNSPublicationPrecondition.Lanes["shadow"] = ref(next, successor)
	second, third := candidate("second"), candidate("third")
	requests := []model.PlatformArtifactReleaseRequest{request(second), request(third)}
	values := []model.PlatformArtifact{second, third}
	errorsSeen := make([]error, 2)
	var workers sync.WaitGroup
	for i := range values {
		workers.Add(1)
		go func(i int) {
			defer workers.Done()
			_, _, _, _, errorsSeen[i] = s.ReleasePlatformArtifact(values[i].ID, requests[i], testPlatformPrincipal())
		}(i)
	}
	workers.Wait()
	passed := 0
	for _, err := range errorsSeen {
		if err == nil {
			passed++
		} else if !errors.Is(err, ErrConflict) {
			t.Fatal("unexpected competing publication error", err)
		}
	}
	if passed != 1 {
		t.Fatal("competing DNS successors did not have exactly one winner", errorsSeen)
	}
	if _, _, _, _, err := s.ReleasePlatformArtifact(next.ID, req, testPlatformPrincipal()); !errors.Is(err, ErrConflict) {
		t.Fatal("superseded retry accepted", err)
	}
	preserve()
}
