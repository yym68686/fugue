package api

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
)

func TestCellProducerPlacementTransitionCapturesAndGuardsExactSource(t *testing.T) {
	st, s, tenantKey, adminKey, app, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	p := cellProducerRoleFixture(t, s, "cell-a", platformconfig.PublicationRoleCellRoutes)
	legacy, err := st.PutEdgeRoutePolicy(model.EdgeRoutePolicy{ID: "source-policy", Hostname: app.Name + ".example.test", AppID: app.ID, TenantID: app.TenantID, EdgeGroupID: "edge-group-original", ExcludedEdgeIDs: []string{"edge-denied"}, RoutePolicy: model.EdgeRoutePolicyEnabled, Enabled: true, MinHealthyEdgeNodes: 2})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	original, err := s.capturePlatformIntentForProducer(ctx, platformProducerPrincipal(), p)
	if err != nil {
		t.Fatal(err)
	}
	var source platformconfig.RoutePolicyConstraint
	for _, rule := range original.Policy.RouteConstraints {
		if rule.ID == legacy.ID {
			source = rule
		}
	}
	if source.Hostname == "" {
		t.Fatal("source not projected")
	}
	previous := original.Intent.EdgeTopology.Clone()
	previous.Cells[0].LegacyGroupID = legacy.EdgeGroupID
	sourceDigest, err := platformproducer.RouteConstraintDigest(source)
	if err != nil {
		t.Fatal(err)
	}
	p.Generation = "explicit-placement-transition"
	p.RoutePlacementTransition = &platformproducer.RoutePlacementTransition{PreviousTopology: previous, NextTopology: original.Intent.EdgeTopology.Clone(), Constraints: []platformproducer.RouteConstraintTransition{{Source: source, SourceDigest: sourceDigest}}}
	scope, _ := platformproducer.PolicyScopeForTarget(p.TargetScope)
	raw, _ := json.Marshal(p)
	content := map[string]any{}
	if err := json.Unmarshal(raw, &content); err != nil {
		t.Fatal(err)
	}
	policyArtifact, err := st.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPolicySnapshot, Scope: model.PlatformArtifactScope{ScopeType: "global", Key: scope}, Generation: p.Generation, Content: content})
	if err != nil {
		t.Fatal(err)
	}
	policyArtifact, err = st.ValidatePlatformArtifact(policyArtifact.ID, []model.PlatformArtifactValidationResult{{Name: "fixture", Pass: true}})
	if err != nil {
		t.Fatal(err)
	}
	path := "/v1/admin/platform-config/routes/project?producer_policy_artifact_id=" + policyArtifact.ID
	response := performJSONRequest(t, s, http.MethodGet, path, adminKey, nil)
	if response.Code != http.StatusOK {
		t.Fatal(response.Body.String())
	}
	var captured platformIntentProjectionResponse
	mustDecodeJSON(t, response, &captured)
	if !reflect.DeepEqual(original.Intent, captured.Intent) || captured.RuntimeSnapshot.PolicyGeneration != captured.Policy.Generation || captured.Policy.Generation == original.Policy.Generation {
		t.Fatal("transition changed intent or failed to bind new policy generation")
	}
	if platformproducer.ValidateRoutePlacementOutput(p, captured.Policy) != nil {
		t.Fatal("API omitted the exact transition")
	}
	if r := performJSONRequest(t, s, http.MethodGet, path, tenantKey, nil); r.Code != http.StatusForbidden {
		t.Fatal("tenant obtained producer configuration")
	}
	if _, _, _, _, err := st.ReleasePlatformArtifact(policyArtifact.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, platformProducerPrincipal()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.reconcilePlatformConfigurationScope(ctx, scope, nil); err != nil {
		t.Fatal("transitioned producer publication", err)
	}
	parent, release, found, err := st.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, p.TargetScope, "shadow")
	if err != nil || !found {
		t.Fatal("transitioned output absent", err)
	}
	for _, kind := range []string{model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindCaddyRouteConfig} {
		child, err := s.consumerAssignmentChild(parent, kind)
		if err != nil {
			t.Fatal(err)
		}
		var payload struct {
			Policy platformconfig.PolicySnapshot `json:"policy"`
		}
		raw, _ := json.Marshal(child.Content)
		if json.Unmarshal(raw, &payload) != nil || platformproducer.ValidateRoutePlacementOutput(p, payload.Policy) != nil {
			t.Fatal("signed child did not retain exact transformed constraint")
		}
	}
	// Bypass capture with a valid but untransformed compiler input. The store
	// must reject it even though the producer signed every derived artifact.
	if _, err := s.reconcilePlatformConfigurationScope(ctx, scope, func(context.Context, model.Principal) (platformIntentProjectionResponse, error) { return original, nil }); err == nil {
		t.Fatal("publication trusted capture instead of validating transformed constraints")
	}
	legacy.MinHealthyEdgeNodes = 3
	if _, err := st.PutEdgeRoutePolicy(legacy); err != nil {
		t.Fatal(err)
	}
	if _, err := s.reconcilePlatformConfigurationScope(ctx, scope, nil); err == nil {
		t.Fatal("changed source policy was silently mapped")
	}
	if r := performJSONRequest(t, s, http.MethodGet, path, adminKey, nil); r.Code == http.StatusOK {
		t.Fatal("preview accepted stale source pin")
	}
	afterParent, afterRelease, found, err := st.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, p.TargetScope, "shadow")
	if err != nil || !found || afterParent.ID != parent.ID || afterRelease.ID != release.ID {
		t.Fatal("failed transition replaced selected predecessor", err)
	}
	for _, lane := range []string{"gray", "full"} {
		if _, _, found, err := st.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, p.TargetScope, lane); err != nil || found {
			t.Fatal("placement configuration published serving authority", lane, err)
		}
	}
	if lkg, err := st.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, p.TargetScope); err != nil || lkg != nil {
		t.Fatal("placement transition invented positive LKG", err)
	}
}
