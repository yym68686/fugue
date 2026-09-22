package api

import (
	"fugue/internal/model"
	"net/http"
	"testing"
	"time"
)

func TestRuntimeFactsQueryFiltersBeforeLimitAndIncludesProducerHistory(t *testing.T) {
	state, s, tenant, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	at := time.Now().UTC()
	e := model.AuditEvent{ID: "runtime-history", ActorType: "bootstrap", ActorID: "producer", Action: "platform_config.serving_verified", TargetType: "platform_release_set", TargetID: "synthetic-parent", CreatedAt: at}
	if err := state.AppendAuditEvent(e); err != nil {
		t.Fatal(err)
	}
	if err := state.AppendAuditEvent(model.AuditEvent{ID: "unrelated-newer", ActorType: "bootstrap", ActorID: "operator", Action: "app.updated", TargetType: "app", CreatedAt: at.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	path := "/v1/admin/platform-state/runtime-facts?release_set_id=synthetic-parent&artifact_kind=release_set&limit=1"
	r := performJSONRequest(t, s, http.MethodGet, path, admin, nil)
	if r.Code != 200 {
		t.Fatal(r.Code, r.Body.String())
	}
	var out struct {
		Facts       []model.AuditEvent `json:"runtime_facts"`
		GeneratedAt time.Time          `json:"generated_at"`
	}
	mustDecodeJSON(t, r, &out)
	if len(out.Facts) != 1 || out.Facts[0].ID != e.ID || out.GeneratedAt.IsZero() {
		t.Fatal(out)
	}
	for _, suffix := range []string{"0", "-1", "1001", "bad", "", "1&limit=2"} {
		r = performJSONRequest(t, s, http.MethodGet, "/v1/admin/platform-state/runtime-facts?limit="+suffix, admin, nil)
		if r.Code != 400 {
			t.Fatal("invalid limit accepted", suffix, r.Code)
		}
	}
	r = performJSONRequest(t, s, http.MethodGet, path, tenant, nil)
	if r.Code != 403 {
		t.Fatal("tenant can read global facts", r.Code)
	}
}
