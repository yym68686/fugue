package api

import (
	"net/http"
	"path/filepath"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestRuntimeSharingGrantExposesRuntimeAndNodeToGrantee(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	owner, err := s.CreateTenant("Owner Tenant")
	if err != nil {
		t.Fatalf("create owner tenant: %v", err)
	}
	grantee, err := s.CreateTenant("Grantee Tenant")
	if err != nil {
		t.Fatalf("create grantee tenant: %v", err)
	}
	_, ownerKey, err := s.CreateAPIKey(owner.ID, "runtime-owner", []string{"runtime.write"})
	if err != nil {
		t.Fatalf("create owner api key: %v", err)
	}
	_, granteeKey, err := s.CreateAPIKey(grantee.ID, "viewer", []string{"project.write"})
	if err != nil {
		t.Fatalf("create grantee api key: %v", err)
	}
	_, nodeSecret, err := s.CreateNodeKey(owner.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	_, runtimeObj, err := s.BootstrapClusterNode(nodeSecret, "shared-node", "https://shared-node.example.com", nil, "", "")
	if err != nil {
		t.Fatalf("bootstrap cluster node: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/runtimes/"+runtimeObj.ID, granteeKey, nil)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("expected status %d before grant, got %d body=%s", http.StatusForbidden, recorder.Code, recorder.Body.String())
	}

	recorder = performJSONRequest(t, server, http.MethodPost, "/v1/runtimes/"+runtimeObj.ID+"/sharing/grants", ownerKey, map[string]any{
		"tenant_id": grantee.ID,
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d for grant, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var grantResponse struct {
		Grant model.RuntimeAccessGrant `json:"grant"`
	}
	mustDecodeJSON(t, recorder, &grantResponse)
	if grantResponse.Grant.RuntimeID != runtimeObj.ID || grantResponse.Grant.TenantID != grantee.ID {
		t.Fatalf("unexpected grant response: %+v", grantResponse.Grant)
	}

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/runtimes/"+runtimeObj.ID+"/sharing", ownerKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d for owner sharing view, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var sharingResponse struct {
		Runtime model.Runtime              `json:"runtime"`
		Grants  []model.RuntimeAccessGrant `json:"grants"`
	}
	mustDecodeJSON(t, recorder, &sharingResponse)
	if sharingResponse.Runtime.ID != runtimeObj.ID || len(sharingResponse.Grants) != 1 {
		t.Fatalf("unexpected sharing response: %+v", sharingResponse)
	}

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/runtimes/"+runtimeObj.ID+"/sharing", granteeKey, nil)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("expected status %d for non-owner sharing view, got %d body=%s", http.StatusForbidden, recorder.Code, recorder.Body.String())
	}

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/runtimes/"+runtimeObj.ID, granteeKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d for shared runtime, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/nodes/"+runtimeObj.ID, granteeKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d for shared node, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	recorder = performJSONRequest(t, server, http.MethodDelete, "/v1/runtimes/"+runtimeObj.ID+"/sharing/grants/"+grantee.ID, ownerKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d for revoke, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var revokeResponse struct {
		Removed bool `json:"removed"`
	}
	mustDecodeJSON(t, recorder, &revokeResponse)
	if !revokeResponse.Removed {
		t.Fatal("expected revoke response to remove grant")
	}

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/runtimes/"+runtimeObj.ID, granteeKey, nil)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("expected status %d after revoke, got %d body=%s", http.StatusForbidden, recorder.Code, recorder.Body.String())
	}
}

func TestRuntimeSharingMutationRequiresOwner(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	owner, err := s.CreateTenant("Mutation Owner")
	if err != nil {
		t.Fatalf("create owner tenant: %v", err)
	}
	other, err := s.CreateTenant("Mutation Other")
	if err != nil {
		t.Fatalf("create other tenant: %v", err)
	}
	third, err := s.CreateTenant("Mutation Third")
	if err != nil {
		t.Fatalf("create third tenant: %v", err)
	}
	_, otherKey, err := s.CreateAPIKey(other.ID, "other-writer", []string{"runtime.write"})
	if err != nil {
		t.Fatalf("create other api key: %v", err)
	}
	_, nodeSecret, err := s.CreateNodeKey(owner.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	_, runtimeObj, err := s.BootstrapClusterNode(nodeSecret, "owner-node", "https://owner-node.example.com", nil, "", "")
	if err != nil {
		t.Fatalf("bootstrap cluster node: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/runtimes/"+runtimeObj.ID+"/sharing/grants", otherKey, map[string]any{
		"tenant_id": third.ID,
	})
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("expected status %d for non-owner grant, got %d body=%s", http.StatusForbidden, recorder.Code, recorder.Body.String())
	}

	recorder = performJSONRequest(t, server, http.MethodPost, "/v1/runtimes/"+runtimeObj.ID+"/sharing/mode", otherKey, map[string]any{
		"access_mode": model.RuntimeAccessModePrivate,
	})
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("expected status %d for non-owner mode update, got %d body=%s", http.StatusForbidden, recorder.Code, recorder.Body.String())
	}
}

func TestPlatformAdminCanManageRuntimeSharingAcrossTenants(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	owner, err := s.CreateTenant("Admin Share Owner")
	if err != nil {
		t.Fatalf("create owner tenant: %v", err)
	}
	grantee, err := s.CreateTenant("Admin Share Grantee")
	if err != nil {
		t.Fatalf("create grantee tenant: %v", err)
	}
	_, nodeSecret, err := s.CreateNodeKey(owner.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	_, runtimeObj, err := s.BootstrapClusterNode(nodeSecret, "admin-shared-node", "https://admin-shared-node.example.com", nil, "", "")
	if err != nil {
		t.Fatalf("bootstrap cluster node: %v", err)
	}

	server := NewServer(s, auth.New(s, "bootstrap-secret"), nil, ServerConfig{})

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/runtimes/"+runtimeObj.ID+"/sharing/grants", "bootstrap-secret", map[string]any{
		"tenant_id": grantee.ID,
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d for platform admin grant, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/runtimes/"+runtimeObj.ID+"/sharing", "bootstrap-secret", nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d for platform admin sharing view, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	recorder = performJSONRequest(t, server, http.MethodPost, "/v1/runtimes/"+runtimeObj.ID+"/sharing/mode", "bootstrap-secret", map[string]any{
		"access_mode": model.RuntimeAccessModePublic,
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d for platform admin mode update, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
}

func TestSetRuntimeAccessModeRequiresPlatformAdmin(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	owner, err := s.CreateTenant("Platform Shared Owner")
	if err != nil {
		t.Fatalf("create owner tenant: %v", err)
	}
	consumer, err := s.CreateTenant("Platform Shared Consumer")
	if err != nil {
		t.Fatalf("create consumer tenant: %v", err)
	}
	_, ownerKey, err := s.CreateAPIKey(owner.ID, "owner-writer", []string{"runtime.write"})
	if err != nil {
		t.Fatalf("create owner writer key: %v", err)
	}
	_, adminKey, err := s.CreateAPIKey(owner.ID, "owner-admin", []string{"platform.admin"})
	if err != nil {
		t.Fatalf("create owner admin key: %v", err)
	}
	_, consumerKey, err := s.CreateAPIKey(consumer.ID, "consumer", []string{"project.write"})
	if err != nil {
		t.Fatalf("create consumer api key: %v", err)
	}
	_, nodeSecret, err := s.CreateNodeKey(owner.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	_, runtimeObj, err := s.BootstrapClusterNode(nodeSecret, "shared-cluster-node", "https://shared-cluster-node.example.com", nil, "", "")
	if err != nil {
		t.Fatalf("bootstrap cluster node: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/runtimes/"+runtimeObj.ID+"/sharing/mode", ownerKey, map[string]any{
		"access_mode": model.RuntimeAccessModePlatformShared,
	})
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("expected status %d for non-admin platform share, got %d body=%s", http.StatusForbidden, recorder.Code, recorder.Body.String())
	}

	recorder = performJSONRequest(t, server, http.MethodPost, "/v1/runtimes/"+runtimeObj.ID+"/sharing/mode", adminKey, map[string]any{
		"access_mode": model.RuntimeAccessModePlatformShared,
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d for admin platform share, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response struct {
		Runtime model.Runtime `json:"runtime"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.Runtime.AccessMode != model.RuntimeAccessModePlatformShared {
		t.Fatalf("expected platform-shared access mode, got %q", response.Runtime.AccessMode)
	}

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/runtimes/"+runtimeObj.ID, consumerKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d for consumer visibility after platform share, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
}

func TestRuntimeOwnerCanSetPublicAccessAndOffer(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	owner, err := s.CreateTenant("Public Offer Owner")
	if err != nil {
		t.Fatalf("create owner tenant: %v", err)
	}
	consumer, err := s.CreateTenant("Public Offer Consumer")
	if err != nil {
		t.Fatalf("create consumer tenant: %v", err)
	}
	_, ownerKey, err := s.CreateAPIKey(owner.ID, "owner-writer", []string{"runtime.write"})
	if err != nil {
		t.Fatalf("create owner api key: %v", err)
	}
	_, consumerKey, err := s.CreateAPIKey(consumer.ID, "consumer", []string{"project.write"})
	if err != nil {
		t.Fatalf("create consumer api key: %v", err)
	}
	_, nodeSecret, err := s.CreateNodeKey(owner.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	_, runtimeObj, err := s.BootstrapClusterNode(nodeSecret, "public-offer-node", "https://public-offer-node.example.com", nil, "", "")
	if err != nil {
		t.Fatalf("bootstrap cluster node: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/runtimes/"+runtimeObj.ID+"/sharing/mode", ownerKey, map[string]any{
		"access_mode": model.RuntimeAccessModePublic,
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d for owner public share, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var accessModeResponse struct {
		Runtime model.Runtime `json:"runtime"`
	}
	mustDecodeJSON(t, recorder, &accessModeResponse)
	if accessModeResponse.Runtime.AccessMode != model.RuntimeAccessModePublic {
		t.Fatalf("expected public access mode, got %q", accessModeResponse.Runtime.AccessMode)
	}

	recorder = performJSONRequest(t, server, http.MethodPost, "/v1/runtimes/"+runtimeObj.ID+"/public-offer", ownerKey, map[string]any{
		"reference_bundle": map[string]any{
			"cpu_millicores":    2000,
			"memory_mebibytes":  4096,
			"storage_gibibytes": 30,
		},
		"reference_monthly_price_microcents": 400 * int64(1_000_000),
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d for public offer update, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var offerResponse struct {
		Runtime model.Runtime `json:"runtime"`
	}
	mustDecodeJSON(t, recorder, &offerResponse)
	if offerResponse.Runtime.PublicOffer == nil {
		t.Fatal("expected runtime public offer in response")
	}
	if offerResponse.Runtime.PublicOffer.ReferenceMonthlyPriceMicroCents != 400*int64(1_000_000) {
		t.Fatalf("unexpected public offer monthly price: %+v", offerResponse.Runtime.PublicOffer)
	}

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/runtimes/"+runtimeObj.ID, consumerKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d for consumer visibility after public share, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
}
