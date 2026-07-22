package api

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/compositecoordinator"
	"fugue/internal/releasecontract"
	"fugue/internal/store"
)

func TestPrepareCompositeReleaseTransactionCreatesExactInertRecord(t *testing.T) {
	stateStore, server, platformAdminKey, tenantKey := newCompositeTransactionAPITestServer(t)
	plan := compositeTransactionAPIPlan(t)
	encoded := compositeTransactionAPIPlanJSON(t, plan)

	forbidden := performJSONRequest(t, server, http.MethodPost, "/v1/admin/composite-release-transactions", tenantKey, map[string]any{
		"planDigest": plan.Digest, "plan": json.RawMessage(encoded),
	})
	if forbidden.Code != http.StatusForbidden {
		t.Fatalf("tenant status=%d body=%s", forbidden.Code, forbidden.Body.String())
	}

	created := performJSONRequest(t, server, http.MethodPost, "/v1/admin/composite-release-transactions", platformAdminKey, map[string]any{
		"planDigest": plan.Digest, "plan": json.RawMessage(encoded),
	})
	if created.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", created.Code, created.Body.String())
	}
	var response struct {
		Record compositecoordinator.Record `json:"record"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Record.State != compositecoordinator.StatePrepared || response.Record.Revision != 1 ||
		response.Record.Plan.Digest != plan.Digest || response.Record.CurrentStep != 0 ||
		response.Record.RollbackStartStep != -1 || response.Record.FailureReason != "" || response.Record.FreezeReason != "" {
		t.Fatalf("prepared record crossed execution boundary: %#v", response.Record)
	}
	persisted, err := stateStore.GetCompositeReleaseTransaction(response.Record.ID)
	if err != nil {
		t.Fatalf("get persisted record: %v", err)
	}
	if persisted.Digest != response.Record.Digest || persisted.Plan.Digest != plan.Digest {
		t.Fatalf("persisted record mismatch: response=%#v persisted=%#v", response.Record, persisted)
	}
	events, err := stateStore.ListAuditEvents("", true, 20)
	if err != nil {
		t.Fatalf("list audit events: %v", err)
	}
	foundAudit := false
	for _, event := range events {
		if event.Action == "composite.transaction.prepare" && event.TargetType == "composite_transaction" &&
			event.TargetID == response.Record.ID && event.Metadata["plan_digest"] == plan.Digest {
			foundAudit = true
		}
	}
	if !foundAudit {
		t.Fatalf("prepared transaction audit event missing: %#v", events)
	}

	duplicate := performJSONRequest(t, server, http.MethodPost, "/v1/admin/composite-release-transactions", platformAdminKey, map[string]any{
		"planDigest": plan.Digest, "plan": json.RawMessage(encoded),
	})
	if duplicate.Code != http.StatusConflict {
		t.Fatalf("duplicate status=%d body=%s", duplicate.Code, duplicate.Body.String())
	}
}

func TestPrepareCompositeReleaseTransactionRejectsUnboundOrNonCanonicalPlan(t *testing.T) {
	_, server, platformAdminKey, _ := newCompositeTransactionAPITestServer(t)
	plan := compositeTransactionAPIPlan(t)
	encoded := compositeTransactionAPIPlanJSON(t, plan)

	wrongDigest := performJSONRequest(t, server, http.MethodPost, "/v1/admin/composite-release-transactions", platformAdminKey, map[string]any{
		"planDigest": compositeTransactionAPIDigest("f"), "plan": json.RawMessage(encoded),
	})
	if wrongDigest.Code != http.StatusBadRequest {
		t.Fatalf("wrong digest status=%d body=%s", wrongDigest.Code, wrongDigest.Body.String())
	}
	whitespaceDigest := performJSONRequest(t, server, http.MethodPost, "/v1/admin/composite-release-transactions", platformAdminKey, map[string]any{
		"planDigest": " " + plan.Digest + " ", "plan": json.RawMessage(encoded),
	})
	if whitespaceDigest.Code != http.StatusBadRequest {
		t.Fatalf("whitespace digest status=%d body=%s", whitespaceDigest.Code, whitespaceDigest.Body.String())
	}

	unknownField := strings.Replace(string(encoded), `"digest":`, `"unexpected":true,"digest":`, 1)
	invalid := performJSONRequest(t, server, http.MethodPost, "/v1/admin/composite-release-transactions", platformAdminKey, map[string]any{
		"planDigest": plan.Digest, "plan": json.RawMessage(unknownField),
	})
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("unknown field status=%d body=%s", invalid.Code, invalid.Body.String())
	}
}

func newCompositeTransactionAPITestServer(t *testing.T) (*store.Store, *Server, string, string) {
	t.Helper()
	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Composite API Test")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, platformAdminKey, err := stateStore.CreateAPIKey(tenant.ID, "platform-admin", []string{"platform.admin"})
	if err != nil {
		t.Fatalf("create platform admin key: %v", err)
	}
	_, tenantKey, err := stateStore.CreateAPIKey(tenant.ID, "tenant", []string{"app.read"})
	if err != nil {
		t.Fatalf("create tenant key: %v", err)
	}
	return stateStore, NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{}), platformAdminKey, tenantKey
}

func compositeTransactionAPIPlan(t *testing.T) releasecontract.CompositeReleasePlan {
	t.Helper()
	plan, err := releasecontract.NewCompositeReleasePlan(releasecontract.CompositeReleasePlan{
		BaseCommit:                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		TargetCommit:              "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		ImageActivationPlanDigest: compositeTransactionAPIDigest("1"),
		Generation:                "1", FencingEpoch: "1",
		BaseVersions: []releasecontract.DomainVersion{
			{Domain: releasecontract.DomainAuthoritativeDNS, Version: compositeTransactionAPIDigest("7")},
			{Domain: releasecontract.DomainControlPlane, Version: compositeTransactionAPIDigest("8")},
		},
		TargetVersions: []releasecontract.DomainVersion{
			{Domain: releasecontract.DomainAuthoritativeDNS, Version: compositeTransactionAPIDigest("9")},
			{Domain: releasecontract.DomainControlPlane, Version: compositeTransactionAPIDigest("a")},
		},
		Steps: []releasecontract.CompositeReleaseStep{
			compositeTransactionAPIStep("authoritative-dns", releasecontract.DomainAuthoritativeDNS, "control_plane_release_adapter_authoritative_dns", nil, "2", "3", "7", "9"),
			compositeTransactionAPIStep("control-plane", releasecontract.DomainControlPlane, "control_plane_release_adapter_control_plane", []string{"authoritative-dns"}, "4", "5", "8", "a"),
		},
	})
	if err != nil {
		t.Fatalf("new composite plan: %v", err)
	}
	return plan
}

func compositeTransactionAPIStep(id string, domain releasecontract.Domain, adapter string, depends []string, forward, reverse, base, target string) releasecontract.CompositeReleaseStep {
	return releasecontract.CompositeReleaseStep{
		ID: id, Domain: domain, Adapter: adapter, DependsOn: depends,
		ActivationIDs: []string{"activation-" + id}, BaseVersion: compositeTransactionAPIDigest(base), TargetVersion: compositeTransactionAPIDigest(target),
		ForwardRenderedDigest: compositeTransactionAPIDigest(forward), ReverseRenderedDigest: compositeTransactionAPIDigest(reverse),
		Observation: releasecontract.CompositeObservationPolicy{
			HealthEvidenceDigest: compositeTransactionAPIDigest("6"), MinimumSamples: "5", WindowSeconds: "120",
		},
		RollbackBudgetSeconds: "300",
	}
}

func compositeTransactionAPIPlanJSON(t *testing.T, plan releasecontract.CompositeReleasePlan) []byte {
	t.Helper()
	encoded, err := releasecontract.MarshalCompositeReleasePlan(plan)
	if err != nil {
		t.Fatalf("marshal composite plan: %v", err)
	}
	return encoded
}

func compositeTransactionAPIDigest(digit string) string {
	return "sha256:" + strings.Repeat(digit, 64)
}
