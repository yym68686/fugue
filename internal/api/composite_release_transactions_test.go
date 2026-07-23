package api

import (
	"bytes"
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

func TestRunCompositeReleaseTransactionNoopCommitsExactTwoDomainRecord(t *testing.T) {
	stateStore, server, platformAdminKey, tenantKey := newCompositeTransactionAPITestServer(t)
	plan := compositeTransactionAPIPlan(t)
	prepared := prepareCompositeTransactionForAPITest(t, server, platformAdminKey, plan)
	envelope := compositeTransactionAPIEnvelopeJSON(t, prepared)
	path := "/v1/admin/composite-release-transactions/" + prepared.ID + "/execute-noop"

	forbidden := performJSONRequest(t, server, http.MethodPost, path, tenantKey, map[string]any{
		"envelope": json.RawMessage(envelope),
	})
	if forbidden.Code != http.StatusForbidden {
		t.Fatalf("tenant status=%d body=%s", forbidden.Code, forbidden.Body.String())
	}
	stillPrepared, err := stateStore.GetCompositeReleaseTransaction(prepared.ID)
	if err != nil || stillPrepared.Revision != prepared.Revision || stillPrepared.Digest != prepared.Digest {
		t.Fatalf("forbidden request changed prepared record: record=%#v err=%v", stillPrepared, err)
	}

	committed := performJSONRequest(t, server, http.MethodPost, path, platformAdminKey, map[string]any{
		"envelope": json.RawMessage(envelope),
	})
	if committed.Code != http.StatusOK {
		t.Fatalf("commit status=%d body=%s", committed.Code, committed.Body.String())
	}
	var response struct {
		Record compositecoordinator.Record               `json:"record"`
		Result compositecoordinator.DurableNoopRunResult `json:"result"`
	}
	if err := json.Unmarshal(committed.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode committed response: %v", err)
	}
	authorization, err := compositecoordinator.DecodeAndAuthorizeNoop(prepared, bytes.NewReader(envelope))
	if err != nil {
		t.Fatalf("rebuild no-op authorization: %v", err)
	}
	if err := compositecoordinator.VerifyDurableNoopRunResult(prepared, authorization, response.Record, response.Result); err != nil {
		t.Fatalf("verify committed no-op result: %v", err)
	}
	if response.Record.State != compositecoordinator.StateCommitted || response.Record.CurrentStep != 2 || response.Record.Revision != 6 ||
		response.Result.ProductionWrite || response.Result.FinalState != compositecoordinator.StateCommitted ||
		len(response.Result.Events) != 4 {
		t.Fatalf("unexpected committed no-op result: record=%#v result=%#v", response.Record, response.Result)
	}
	wantedActions := []string{"apply-noop", "observe-noop", "apply-noop", "observe-noop"}
	wantedSteps := []string{plan.Steps[0].ID, plan.Steps[0].ID, plan.Steps[1].ID, plan.Steps[1].ID}
	for index := range wantedActions {
		if response.Result.Events[index].Action != wantedActions[index] || response.Result.Events[index].StepID != wantedSteps[index] {
			t.Fatalf("event %d=%#v, want %s/%s", index, response.Result.Events[index], wantedActions[index], wantedSteps[index])
		}
	}
	persisted, err := stateStore.GetCompositeReleaseTransaction(prepared.ID)
	if err != nil || persisted.Digest != response.Record.Digest || persisted.Revision != 6 {
		t.Fatalf("committed record was not durable: record=%#v err=%v", persisted, err)
	}

	replayed := performJSONRequest(t, server, http.MethodPost, path, platformAdminKey, map[string]any{
		"envelope": json.RawMessage(envelope),
	})
	if replayed.Code != http.StatusConflict {
		t.Fatalf("replay status=%d body=%s", replayed.Code, replayed.Body.String())
	}
	afterReplay, err := stateStore.GetCompositeReleaseTransaction(prepared.ID)
	if err != nil || afterReplay.Digest != persisted.Digest || afterReplay.Revision != persisted.Revision {
		t.Fatalf("replay changed committed record: before=%#v after=%#v err=%v", persisted, afterReplay, err)
	}

	events, err := stateStore.ListAuditEvents("", true, 20)
	if err != nil {
		t.Fatalf("list audit events: %v", err)
	}
	foundAudit := false
	for _, event := range events {
		if event.Action == "composite.transaction.noop.commit" && event.TargetID == prepared.ID &&
			event.Metadata["result_digest"] == response.Result.Digest && event.Metadata["production_write"] == "false" {
			foundAudit = true
		}
	}
	if !foundAudit {
		t.Fatalf("committed no-op audit event missing: %#v", events)
	}
}

func TestRunCompositeReleaseTransactionNoopFailsClosedBeforeAdvance(t *testing.T) {
	t.Run("mismatched envelope", func(t *testing.T) {
		stateStore, server, platformAdminKey, _ := newCompositeTransactionAPITestServer(t)
		plan := compositeTransactionAPIPlan(t)
		prepared := prepareCompositeTransactionForAPITest(t, server, platformAdminKey, plan)
		envelope := compositeTransactionAPIEnvelopeJSON(t, prepared)
		tampered := strings.Replace(string(envelope), prepared.Digest, compositeTransactionAPIDigest("f"), 1)

		response := performJSONRequest(t, server, http.MethodPost,
			"/v1/admin/composite-release-transactions/"+prepared.ID+"/execute-noop", platformAdminKey,
			map[string]any{"envelope": json.RawMessage(tampered)},
		)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("tampered status=%d body=%s", response.Code, response.Body.String())
		}
		persisted, err := stateStore.GetCompositeReleaseTransaction(prepared.ID)
		if err != nil || persisted.State != compositecoordinator.StatePrepared || persisted.Revision != 1 || persisted.Digest != prepared.Digest {
			t.Fatalf("tampered envelope advanced record: record=%#v err=%v", persisted, err)
		}
	})

	t.Run("more than two domains", func(t *testing.T) {
		stateStore, server, platformAdminKey, _ := newCompositeTransactionAPITestServer(t)
		plan := compositeTransactionAPIThreeDomainPlan(t)
		prepared := prepareCompositeTransactionForAPITest(t, server, platformAdminKey, plan)
		envelope := compositeTransactionAPIEnvelopeJSON(t, prepared)

		response := performJSONRequest(t, server, http.MethodPost,
			"/v1/admin/composite-release-transactions/"+prepared.ID+"/execute-noop", platformAdminKey,
			map[string]any{"envelope": json.RawMessage(envelope)},
		)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("three-domain status=%d body=%s", response.Code, response.Body.String())
		}
		persisted, err := stateStore.GetCompositeReleaseTransaction(prepared.ID)
		if err != nil || persisted.State != compositecoordinator.StatePrepared || persisted.Revision != 1 || persisted.Digest != prepared.Digest {
			t.Fatalf("three-domain request advanced record: record=%#v err=%v", persisted, err)
		}
	})
}

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

func compositeTransactionAPIThreeDomainPlan(t *testing.T) releasecontract.CompositeReleasePlan {
	t.Helper()
	plan := compositeTransactionAPIPlan(t)
	plan.BaseVersions = append(plan.BaseVersions, releasecontract.DomainVersion{
		Domain: releasecontract.DomainBackup, Version: compositeTransactionAPIDigest("b"),
	})
	plan.TargetVersions = append(plan.TargetVersions, releasecontract.DomainVersion{
		Domain: releasecontract.DomainBackup, Version: compositeTransactionAPIDigest("c"),
	})
	plan.Steps = append(plan.Steps, compositeTransactionAPIStep(
		"backup", releasecontract.DomainBackup, "control_plane_release_adapter_backup",
		[]string{"control-plane"}, "d", "e", "b", "c",
	))
	plan.Digest = ""
	plan, err := releasecontract.NewCompositeReleasePlan(plan)
	if err != nil {
		t.Fatalf("new three-domain composite plan: %v", err)
	}
	return plan
}

func prepareCompositeTransactionForAPITest(
	t *testing.T,
	server *Server,
	platformAdminKey string,
	plan releasecontract.CompositeReleasePlan,
) compositecoordinator.Record {
	t.Helper()
	created := performJSONRequest(t, server, http.MethodPost, "/v1/admin/composite-release-transactions", platformAdminKey, map[string]any{
		"planDigest": plan.Digest, "plan": json.RawMessage(compositeTransactionAPIPlanJSON(t, plan)),
	})
	if created.Code != http.StatusCreated {
		t.Fatalf("prepare status=%d body=%s", created.Code, created.Body.String())
	}
	var response struct {
		Record compositecoordinator.Record `json:"record"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode prepared response: %v", err)
	}
	return response.Record
}

func compositeTransactionAPIEnvelopeJSON(t *testing.T, record compositecoordinator.Record) []byte {
	t.Helper()
	binding := releasecontract.CompositeTransactionBindingForRecord(record.ID, record.Digest, record.Revision, record.Plan)
	envelope, err := releasecontract.NewCompositeTransactionEnvelope(record.Plan, binding)
	if err != nil {
		t.Fatalf("new composite transaction envelope: %v", err)
	}
	encoded, err := releasecontract.MarshalCompositeTransactionEnvelope(envelope)
	if err != nil {
		t.Fatalf("marshal composite transaction envelope: %v", err)
	}
	return encoded
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
