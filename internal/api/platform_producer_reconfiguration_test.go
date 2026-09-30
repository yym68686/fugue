package api

import (
	"net/http"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestProducerReconfigurationAPIRejectsNullAndForeignArtifactWithoutWrites(t *testing.T) {
	st, s, tenant, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	artifact := createTestStaticIntent(t, s, "reconfiguration-api", "http://origin:8080")
	path := "/v1/admin/artifacts/" + artifact.ID + "/release"
	for _, body := range []map[string]any{
		{"release_channel": "shadow", "producer_reconfiguration": nil},
		{"release_channel": "shadow", "producer_reconfiguration": map[string]any{}},
		{"release_channel": "shadow", "producer_reconfiguration": map[string]any{"unrecognized": true}},
	} {
		r := performJSONRequest(t, s, http.MethodPost, path, admin, body)
		if r.Code != http.StatusBadRequest {
			t.Fatalf("invalid CAS status=%d body=%s", r.Code, r.Body.String())
		}
	}
	r := performJSONRequest(t, s, http.MethodPost, path, tenant, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow", ProducerReconfiguration: &model.PlatformProducerReconfiguration{}})
	if r.Code != http.StatusForbidden {
		t.Fatal("tenant reached configuration publication", r.Code)
	}
	ledger, err := st.ListPlatformReleaseMessages(artifact.ArtifactKind, artifact.ScopeKey, time.Time{}, 100)
	if err != nil || len(ledger) != 0 {
		t.Fatal("rejected requests wrote release messages", err)
	}
}
