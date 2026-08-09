package edgecontrol

import (
	"context"
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

type countingAuthorityStatusStore struct {
	AuthorityStatusStore
	calls map[string]int
}

func (store *countingAuthorityStatusStore) ReadGroupAuthorityStatus(ctx context.Context, groupID string) (AuthorityGroupStoreSnapshot, error) {
	store.calls[groupID]++
	return store.AuthorityStatusStore.ReadGroupAuthorityStatus(ctx, groupID)
}
