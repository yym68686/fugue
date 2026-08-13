package edgecontrol

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAuthorityStatusReadsEachGroupStateExactlyOnce(t *testing.T) {
	t.Parallel()

	groups := []string{"edge-group-country-de", "edge-group-country-us", "edge-group-region-test"}
	store, err := OpenPersistentGroupStore(privateStateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	counted := &countingAuthorityStatusStore{AuthorityStatusStore: store, calls: make(map[string]int)}
	now := time.Date(2026, 8, 9, 7, 0, 0, 0, time.UTC)
	handler, err := NewAuthorityStatusHandler(counted, groups, NewAuthorityRuntimeState(func() time.Time { return now }), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, AuthorityStatusPathV1, nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("authority status code = %d, want %d", recorder.Code, http.StatusOK)
	}
	for _, groupID := range groups {
		if got := counted.calls[groupID]; got != 1 {
			t.Fatalf("group %s state reads = %d, want 1", groupID, got)
		}
	}
}

func TestAuthorityStatusExposesExactWorkerCandidateStageCAS(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 13, 9, 0, 0, 0, time.UTC)
	groupID := "edge-group-region-test"
	store, _, candidate, authority := groupPromotionFixture(t, groupID, now)
	handler, err := NewAuthorityStatusHandler(store, []string{groupID}, NewAuthorityRuntimeState(func() time.Time { return now }), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, AuthorityStatusPathV1, nil))
	var status AuthorityStatusSnapshot
	if recorder.Code != http.StatusOK || json.Unmarshal(recorder.Body.Bytes(), &status) != nil || len(status.Groups) != 1 {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	group := status.Groups[0]
	if group.AuthoritySequence != authority.LedgerHead.Sequence || group.PublicationSequence != authority.LedgerHead.Sequence ||
		group.CurrentPublicationSequence != authority.Published.PublicationSequence || group.CandidateEpoch != candidate.Epoch ||
		group.PublishedBundleDigest != authority.Published.Digest || group.RecoveryEpoch != authority.Published.RecoveryEpoch {
		t.Fatalf("stage CAS status=%+v authority=%+v candidate=%+v", group, authority, candidate)
	}
}

type countingAuthorityStatusStore struct {
	AuthorityStatusStore
	calls map[string]int
}

func (store *countingAuthorityStatusStore) ReadGroupAuthorityStatus(ctx context.Context, groupID string) (AuthorityGroupStoreSnapshot, error) {
	store.calls[groupID]++
	return store.AuthorityStatusStore.ReadGroupAuthorityStatus(ctx, groupID)
}
