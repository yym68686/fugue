package bundleauth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

// These types are the signed route-bundle schema consumed by the production
// 7bf04c33 edge. Keep this fixture independent from the current model so a
// future field addition cannot silently change the N-1 compatibility test.
type edgeRouteBundle7bf struct {
	SchemaVersion      string                     `json:"schema_version,omitempty"`
	Version            string                     `json:"version"`
	Generation         string                     `json:"generation,omitempty"`
	PreviousGeneration string                     `json:"previous_generation,omitempty"`
	GeneratedAt        time.Time                  `json:"generated_at"`
	ValidUntil         time.Time                  `json:"valid_until,omitempty"`
	Issuer             string                     `json:"issuer,omitempty"`
	KeyID              string                     `json:"key_id,omitempty"`
	Signature          string                     `json:"signature,omitempty"`
	Signatures         []bundleSignature7bf       `json:"signatures,omitempty"`
	EdgeID             string                     `json:"edge_id,omitempty"`
	EdgeGroupID        string                     `json:"edge_group_id,omitempty"`
	Routes             []edgeRouteBinding7bf      `json:"routes"`
	TLSAllowlist       []edgeTLSAllowlistEntry7bf `json:"tls_allowlist"`
	CachePolicies      []cachePolicy7bf           `json:"cache_policies,omitempty"`
}

type edgeRouteBinding7bf struct {
	Hostname             string                     `json:"hostname"`
	PathPrefix           string                     `json:"path_prefix,omitempty"`
	RouteKind            string                     `json:"route_kind"`
	AppID                string                     `json:"app_id"`
	TenantID             string                     `json:"tenant_id"`
	RuntimeID            string                     `json:"runtime_id"`
	RuntimeType          string                     `json:"runtime_type,omitempty"`
	RuntimeEdgeGroup     string                     `json:"runtime_edge_group,omitempty"`
	RuntimeEdgeGroupID   string                     `json:"runtime_edge_group_id,omitempty"`
	RuntimeClusterNode   string                     `json:"runtime_cluster_node,omitempty"`
	SelectedEdgeGroup    string                     `json:"selected_edge_group,omitempty"`
	EdgeGroupID          string                     `json:"edge_group_id"`
	FallbackEdgeGroupID  string                     `json:"fallback_edge_group_id,omitempty"`
	PolicyEdgeGroupID    string                     `json:"policy_edge_group_id,omitempty"`
	ExcludedEdgeIDs      []string                   `json:"excluded_edge_ids,omitempty"`
	ExcludedEdgeGroupIDs []string                   `json:"excluded_edge_group_ids,omitempty"`
	ExclusionReason      string                     `json:"exclusion_reason,omitempty"`
	ExclusionExpiresAt   *time.Time                 `json:"exclusion_expires_at,omitempty"`
	MinHealthyEdgeNodes  int                        `json:"min_healthy_edge_nodes,omitempty"`
	HealthyEdgeNodeCount int                        `json:"healthy_edge_node_count,omitempty"`
	EdgeRedundancyStatus string                     `json:"edge_redundancy_status,omitempty"`
	EdgeRedundancyReason string                     `json:"edge_redundancy_reason,omitempty"`
	RoutePolicy          string                     `json:"route_policy"`
	SelectionReason      string                     `json:"selection_reason,omitempty"`
	FallbackReason       string                     `json:"fallback_reason,omitempty"`
	UpstreamKind         string                     `json:"upstream_kind"`
	UpstreamScope        string                     `json:"upstream_scope,omitempty"`
	UpstreamURL          string                     `json:"upstream_url,omitempty"`
	Upstreams            []edgeRouteUpstream7bf     `json:"upstreams,omitempty"`
	ServicePort          int                        `json:"service_port"`
	TLSPolicy            string                     `json:"tls_policy"`
	CachePolicyID        string                     `json:"cache_policy_id,omitempty"`
	CacheNamespace       string                     `json:"cache_namespace,omitempty"`
	DeploymentGeneration string                     `json:"deployment_generation,omitempty"`
	RequestBodyPolicies  []edgeRequestBodyPolicy7bf `json:"request_body_policies,omitempty"`
	Streaming            bool                       `json:"streaming"`
	Status               string                     `json:"status"`
	StatusReason         string                     `json:"status_reason,omitempty"`
	RouteGeneration      string                     `json:"route_generation"`
	CreatedAt            time.Time                  `json:"created_at"`
	UpdatedAt            time.Time                  `json:"updated_at"`
}

type edgeRouteUpstream7bf struct {
	Role                 string `json:"role,omitempty"`
	ReleaseID            string `json:"release_id,omitempty"`
	Weight               int    `json:"weight"`
	UpstreamKind         string `json:"upstream_kind,omitempty"`
	UpstreamScope        string `json:"upstream_scope,omitempty"`
	UpstreamURL          string `json:"upstream_url"`
	ServicePort          int    `json:"service_port,omitempty"`
	RuntimeID            string `json:"runtime_id,omitempty"`
	DeploymentGeneration string `json:"deployment_generation,omitempty"`
	Status               string `json:"status,omitempty"`
	StatusReason         string `json:"status_reason,omitempty"`
}

type edgeRequestBodyPolicy7bf struct {
	Name              string   `json:"name"`
	Methods           []string `json:"methods"`
	Paths             []string `json:"paths"`
	MaxBytes          int64    `json:"max_bytes"`
	TimeoutSeconds    int      `json:"timeout_seconds"`
	MaxConcurrent     int      `json:"max_concurrent"`
	RetryAfterSeconds int      `json:"retry_after_seconds,omitempty"`
}

type edgeTLSAllowlistEntry7bf struct {
	Hostname  string `json:"hostname"`
	AppID     string `json:"app_id"`
	TenantID  string `json:"tenant_id"`
	Status    string `json:"status"`
	TLSStatus string `json:"tls_status,omitempty"`
}

type cachePolicy7bf struct {
	ID                          string   `json:"id"`
	Kind                        string   `json:"kind"`
	HostnameScope               string   `json:"hostname_scope,omitempty"`
	PathPatterns                []string `json:"path_patterns,omitempty"`
	MethodAllowlist             []string `json:"method_allowlist,omitempty"`
	StatusAllowlist             []int    `json:"status_allowlist,omitempty"`
	TTLSeconds                  int      `json:"ttl_seconds,omitempty"`
	StaleWhileRevalidateSeconds int      `json:"stale_while_revalidate_seconds,omitempty"`
	BrowserCacheControl         string   `json:"browser_cache_control,omitempty"`
	EdgeCacheControl            string   `json:"edge_cache_control,omitempty"`
	BypassOnAuthorization       bool     `json:"bypass_on_authorization,omitempty"`
	BypassOnCookie              bool     `json:"bypass_on_cookie,omitempty"`
	VaryAllowlist               []string `json:"vary_allowlist,omitempty"`
	PurgeMode                   string   `json:"purge_mode,omitempty"`
}

type bundleSignature7bf struct {
	SchemaVersion      string    `json:"schema_version,omitempty"`
	Issuer             string    `json:"issuer,omitempty"`
	KeyID              string    `json:"key_id,omitempty"`
	Signature          string    `json:"signature,omitempty"`
	GeneratedAt        time.Time `json:"generated_at,omitempty"`
	ValidUntil         time.Time `json:"valid_until,omitempty"`
	PreviousGeneration string    `json:"previous_generation,omitempty"`
}

type bundleSigningPayload7bf struct {
	SchemaVersion      string                     `json:"schema_version,omitempty"`
	Version            string                     `json:"version,omitempty"`
	Generation         string                     `json:"generation,omitempty"`
	PreviousGeneration string                     `json:"previous_generation,omitempty"`
	GeneratedAt        time.Time                  `json:"generated_at,omitempty"`
	ValidUntil         time.Time                  `json:"valid_until,omitempty"`
	Issuer             string                     `json:"issuer,omitempty"`
	KeyID              string                     `json:"key_id,omitempty"`
	EdgeID             string                     `json:"edge_id,omitempty"`
	EdgeGroupID        string                     `json:"edge_group_id,omitempty"`
	Routes             []edgeRouteBinding7bf      `json:"routes,omitempty"`
	TLSAllowlist       []edgeTLSAllowlistEntry7bf `json:"tls_allowlist,omitempty"`
}

func TestEdgeRouteBundleRemainsVerifiableByExact7bfDecoder(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 1, 13, 22, 0, 0, time.UTC)
	key := "route-signing-key"
	keyID := "control-plane"
	bundle := n1CompatibilityBundle(now)
	signed := SignEdgeRouteBundle(bundle, key, keyID, 24*time.Hour)
	legacy := decodeEdgeRouteBundle7bf(t, signed)

	if err := verifyEdgeRouteBundle7bf(legacy, key, keyID, now); err != nil {
		t.Fatalf("7bf04c33 decoder/verifier rejected current signed bundle: %v", err)
	}

	// This is the exact d284 failure shape: only the signature over the current
	// model is present. The 7bf decoder drops decision_id before verification.
	currentOnly := signed
	currentOnly.Signatures = append([]model.BundleSignature(nil), signed.Signatures[:1]...)
	if err := verifyEdgeRouteBundle7bf(decodeEdgeRouteBundle7bf(t, currentOnly), key, keyID, now); !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("7bf negative control accepted a current-model-only signature: %v", err)
	}

	for name, tamper := range map[string]func(*edgeRouteBinding7bf){
		"upstream URL": func(route *edgeRouteBinding7bf) { route.UpstreamURL = "http://attacker.invalid:80" },
		"edge group":   func(route *edgeRouteBinding7bf) { route.EdgeGroupID = "edge-group-attacker" },
		"route status": func(route *edgeRouteBinding7bf) { route.Status = model.EdgeRouteStatusUnavailable },
		"generation":   func(route *edgeRouteBinding7bf) { route.RouteGeneration = "routegen_attacker" },
	} {
		t.Run(name, func(t *testing.T) {
			tampered := cloneEdgeRouteBundle7bf(t, legacy)
			tamper(&tampered.Routes[0])
			if err := verifyEdgeRouteBundle7bf(tampered, key, keyID, now); !errors.Is(err, ErrInvalidSignature) {
				t.Fatalf("7bf verifier accepted tampered %s: %v", name, err)
			}
		})
	}
}

func TestEdgeRoute7bfCompatibilityProjectionOnlyDropsAllowlistedNonSecurityFields(t *testing.T) {
	t.Parallel()

	current := jsonFieldNames(reflect.TypeOf(model.EdgeRouteBinding{}))
	legacy := jsonFieldNames(reflect.TypeOf(edgeRouteBinding7bf{}))
	removed := stringSetDifference(current, legacy)
	unexpectedLegacy := stringSetDifference(legacy, current)

	// decision_id correlates a decision with observability evidence. It does not
	// select an origin, edge group, policy, status, generation, or TLS behavior.
	allowedNonSecurityProjection := []string{"decision_id"}
	if !reflect.DeepEqual(removed, allowedNonSecurityProjection) {
		t.Fatalf("7bf compatibility projection changed: removed=%v allowed=%v", removed, allowedNonSecurityProjection)
	}
	if len(unexpectedLegacy) != 0 {
		t.Fatalf("7bf fixture contains fields absent from the current route model: %v", unexpectedLegacy)
	}
}

func n1CompatibilityBundle(now time.Time) model.EdgeRouteBundle {
	return model.EdgeRouteBundle{
		SchemaVersion: model.BundleSchemaVersionV1,
		Version:       "routegen_n1",
		Generation:    "routegen_n1",
		GeneratedAt:   now,
		Issuer:        "fugue-control-plane",
		EdgeID:        "edge-us-1",
		EdgeGroupID:   "edge-group-country-us",
		Routes: []model.EdgeRouteBinding{{
			Hostname:             "api.example.test",
			RouteKind:            model.EdgeRouteKindPlatform,
			AppID:                "app_1",
			TenantID:             "tenant_1",
			RuntimeID:            "runtime_1",
			RuntimeType:          "kubernetes",
			EdgeGroupID:          "edge-group-country-us",
			RoutePolicy:          model.EdgeRoutePolicyEnabled,
			UpstreamKind:         model.EdgeRouteUpstreamKindKubernetesService,
			UpstreamScope:        model.EdgeRouteUpstreamScopeCluster,
			UpstreamURL:          "http://api.default.svc.cluster.local:80",
			ServicePort:          80,
			TLSPolicy:            model.EdgeRouteTLSPolicyPlatform,
			DeploymentGeneration: "image-generation-1",
			Streaming:            true,
			Status:               model.EdgeRouteStatusActive,
			StatusReason:         "ready",
			DecisionID:           "decision_n1",
			RouteGeneration:      "route_binding_n1",
			CreatedAt:            now,
			UpdatedAt:            now,
		}},
		TLSAllowlist: []model.EdgeTLSAllowlistEntry{{
			Hostname:  "api.example.test",
			AppID:     "app_1",
			TenantID:  "tenant_1",
			Status:    "active",
			TLSStatus: "ready",
		}},
	}
}

func decodeEdgeRouteBundle7bf(t *testing.T, bundle model.EdgeRouteBundle) edgeRouteBundle7bf {
	t.Helper()
	raw, err := json.Marshal(bundle)
	if err != nil {
		t.Fatalf("marshal current route bundle: %v", err)
	}
	var legacy edgeRouteBundle7bf
	if err := json.Unmarshal(raw, &legacy); err != nil {
		t.Fatalf("decode route bundle with 7bf schema: %v", err)
	}
	return legacy
}

func cloneEdgeRouteBundle7bf(t *testing.T, bundle edgeRouteBundle7bf) edgeRouteBundle7bf {
	t.Helper()
	raw, err := json.Marshal(bundle)
	if err != nil {
		t.Fatalf("marshal 7bf route bundle: %v", err)
	}
	var cloned edgeRouteBundle7bf
	if err := json.Unmarshal(raw, &cloned); err != nil {
		t.Fatalf("clone 7bf route bundle: %v", err)
	}
	return cloned
}

func verifyEdgeRouteBundle7bf(bundle edgeRouteBundle7bf, key, keyID string, now time.Time) error {
	return verifyEdgeRouteBundle7bfWithKeyring(bundle, NewKeyring(key, keyID, "", "", nil), now)
}

func verifyEdgeRouteBundle7bfWithKeyring(bundle edgeRouteBundle7bf, keyring Keyring, now time.Time) error {
	if err := validateBundleSchemaVersion(bundle.SchemaVersion); err != nil {
		return err
	}
	if !bundle.ValidUntil.IsZero() && now.After(bundle.ValidUntil) {
		return ErrExpiredBundle
	}
	keyring = NewKeyring(
		keyring.PrimaryKey,
		keyring.PrimaryKeyID,
		keyring.PreviousKey,
		keyring.PreviousKeyID,
		keyring.revokedKeyIDs(),
	)
	candidates := []bundleSignature7bf{{
		KeyID:      bundle.KeyID,
		Signature:  bundle.Signature,
		ValidUntil: bundle.ValidUntil,
	}}
	candidates = append(candidates, bundle.Signatures...)
	if keyring.primaryKey() == "" && keyring.previousKey() == "" {
		return nil
	}
	foundSignature := false
	matchedKey := false
	invalidSignature := false
	for _, candidate := range candidates {
		candidateKeyID := strings.TrimSpace(candidate.KeyID)
		signature := strings.TrimSpace(candidate.Signature)
		if candidateKeyID == "" || signature == "" {
			continue
		}
		foundSignature = true
		if keyring.isRevoked(candidateKeyID) {
			continue
		}
		key, ok := keyring.keyForID(candidateKeyID)
		if !ok || key == "" {
			continue
		}
		matchedKey = true
		payload := bundleSigningPayload7bf{
			SchemaVersion:      bundle.SchemaVersion,
			Version:            bundle.Version,
			Generation:         bundle.Generation,
			PreviousGeneration: bundle.PreviousGeneration,
			GeneratedAt:        bundle.GeneratedAt,
			ValidUntil:         candidate.ValidUntil,
			Issuer:             bundle.Issuer,
			KeyID:              candidateKeyID,
			EdgeID:             bundle.EdgeID,
			EdgeGroupID:        bundle.EdgeGroupID,
			Routes:             bundle.Routes,
			TLSAllowlist:       bundle.TLSAllowlist,
		}
		raw, _ := json.Marshal(payload)
		mac := hmac.New(sha256.New, []byte(key))
		_, _ = mac.Write(raw)
		expected := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
		if hmac.Equal([]byte(signature), []byte(expected)) {
			return nil
		}
		invalidSignature = true
	}
	if !foundSignature {
		return ErrMissingSignature
	}
	if invalidSignature || matchedKey {
		return ErrInvalidSignature
	}
	if bundle.KeyID != "" {
		return fmt.Errorf("%w: got %s", ErrKeyIDMismatch, strings.TrimSpace(bundle.KeyID))
	}
	return ErrInvalidSignature
}

func jsonFieldNames(typ reflect.Type) []string {
	fields := make([]string, 0, typ.NumField())
	for index := 0; index < typ.NumField(); index++ {
		name := strings.Split(typ.Field(index).Tag.Get("json"), ",")[0]
		if name != "" && name != "-" {
			fields = append(fields, name)
		}
	}
	sort.Strings(fields)
	return fields
}

func stringSetDifference(left, right []string) []string {
	rightSet := make(map[string]struct{}, len(right))
	for _, value := range right {
		rightSet[value] = struct{}{}
	}
	out := make([]string, 0)
	for _, value := range left {
		if _, ok := rightSet[value]; !ok {
			out = append(out, value)
		}
	}
	sort.Strings(out)
	return out
}
