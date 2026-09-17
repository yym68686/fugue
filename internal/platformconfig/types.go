package platformconfig

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

const (
	SchemaVersion   = "fugue.platform.config/v1"
	CompilerVersion = "platform-config-compiler/v17"
	GlobalScopeKey  = "global"
)

// PlatformIntent is the versioned description of what Fugue should serve.
// It intentionally contains no runtime health, ACK, or observed state.
type PlatformIntent struct {
	ACMEChallenges []ACMEChallengeIntent `json:"acme_challenges,omitempty"`
	SchemaVersion  string                `json:"schema_version"`
	Generation     string                `json:"generation"`
	Scope          string                `json:"scope"`
	Routes         []RouteIntent         `json:"routes,omitempty"`
	DNS            []DNSIntent           `json:"dns,omitempty"`
	TLS            []TLSIntent           `json:"tls,omitempty"`
	CachePolicies  []model.CachePolicy   `json:"cache_policies,omitempty"`
	CreatedAt      time.Time             `json:"created_at,omitempty"`
}

type RouteIntent struct {
	Hostname             string                        `json:"hostname"`
	Kind                 string                        `json:"kind,omitempty"`
	UpstreamKind         string                        `json:"upstream_kind,omitempty"`
	UpstreamScope        string                        `json:"upstream_scope,omitempty"`
	UpstreamURL          string                        `json:"upstream_url"`
	TLSPolicy            string                        `json:"tls_policy,omitempty"`
	RoutePolicy          string                        `json:"route_policy,omitempty"`
	EdgeGroupMode        string                        `json:"edge_group_mode,omitempty"`
	Enabled              bool                          `json:"enabled"`
	EdgeGroupID          string                        `json:"edge_group_id,omitempty"`
	TTL                  int                           `json:"ttl,omitempty"`
	Status               string                        `json:"status,omitempty"`
	StatusReason         string                        `json:"status_reason,omitempty"`
	PathPrefix           string                        `json:"path_prefix,omitempty"`
	ServicePort          int                           `json:"service_port,omitempty"`
	Streaming            *bool                         `json:"streaming,omitempty"`
	Upstreams            []UpstreamIntent              `json:"upstreams,omitempty"`
	CachePolicyID        string                        `json:"cache_policy_id,omitempty"`
	CacheNamespace       string                        `json:"cache_namespace,omitempty"`
	DeploymentGeneration string                        `json:"deployment_generation,omitempty"`
	RequestBodyPolicies  []model.EdgeRequestBodyPolicy `json:"request_body_policies,omitempty"`
	AppID                string                        `json:"app_id,omitempty"`
	TenantID             string                        `json:"tenant_id,omitempty"`
	RuntimeID            string                        `json:"runtime_id,omitempty"`
	OriginRef            string                        `json:"origin_ref,omitempty"`
}

type DNSIntent struct {
	Route               *DNSRouteIntent       `json:"route,omitempty"`
	Application         *DNSApplicationIntent `json:"application,omitempty"`
	ValueExpirations    map[string]time.Time  `json:"value_expirations,omitempty"`
	Flatten             *DNSFlattenIntent     `json:"flatten,omitempty"`
	Hostname            string                `json:"hostname"`
	Type                string                `json:"type"`
	Values              []string              `json:"values"`
	TTL                 int                   `json:"ttl"`
	RecordKind          string                `json:"record_kind,omitempty"`
	Status              string                `json:"status,omitempty"`
	StatusReason        string                `json:"status_reason,omitempty"`
	AppID               string                `json:"app_id,omitempty"`
	TenantID            string                `json:"tenant_id,omitempty"`
	EdgeGroupID         string                `json:"edge_group_id,omitempty"`
	FallbackEdgeGroupID string                `json:"fallback_edge_group_id,omitempty"`
}

// DNSApplicationIntent preserves desired policies for a symbolic app binding.
// Selected addresses and readiness must be captured separately as runtime facts.
type DNSApplicationIntent struct {
	IPv4Policy     string `json:"ipv4_policy"`
	IPv6Policy     string `json:"ipv6_policy"`
	TTLPolicy      string `json:"ttl_policy"`
	FallbackPolicy string `json:"fallback_policy"`
}

// DNSRouteIntent declares the routes required by an address RRset, including
// aliases whose DNS owner differs from the HTTP/TLS hostnames they represent.
type DNSRouteIntent struct {
	DNSApplicationIntent
	Hostnames []string          `json:"hostnames"`
	Bindings  []DNSRouteBinding `json:"bindings,omitempty"`
}

// DNSRouteBinding is an explicit hostname/path/app dependency for a route
// target shared by applications in one tenant. It does not duplicate route
// configuration; the compiler checks it against PlatformIntent routes.
type DNSRouteBinding struct {
	Hostname   string `json:"hostname"`
	PathPrefix string `json:"path_prefix"`
	AppID      string `json:"app_id"`
}

type DNSFlattenIntent struct {
	Zone           string `json:"zone,omitempty"`
	Mode           string `json:"mode"`
	Target         string `json:"target"`
	IPv4Policy     string `json:"ipv4_policy"`
	IPv6Policy     string `json:"ipv6_policy"`
	TTLPolicy      string `json:"ttl_policy"`
	FallbackPolicy string `json:"fallback_policy"`
}

type TLSIntent struct {
	Hostname string `json:"hostname"`
	Policy   string `json:"policy"`
}

// PolicySnapshot contains changeable release constraints. It is deliberately
// typed and bounded; it is not an arbitrary executable policy language.
type PolicySnapshot struct {
	SchemaVersion       string                    `json:"schema_version"`
	Generation          string                    `json:"generation"`
	Scope               string                    `json:"scope"`
	RequireTLSReady     bool                      `json:"require_tls_ready"`
	RequireRouteReady   bool                      `json:"require_route_ready"`
	MinimumHealthyEdges int                       `json:"minimum_healthy_edges"`
	MaxStaleSeconds     int                       `json:"max_stale_seconds"`
	CanaryWeights       []int                     `json:"canary_weights,omitempty"`
	DependencyOrder     []string                  `json:"dependency_order,omitempty"`
	ConstraintGraph     ConstraintGraph           `json:"constraint_graph,omitempty"`
	RouteConstraints    []RoutePolicyConstraint   `json:"route_constraints,omitempty"`
	TrafficConstraints  []TrafficPolicyConstraint `json:"traffic_constraints,omitempty"`
	CreatedAt           time.Time                 `json:"created_at,omitempty"`
}

type ConstraintGraph struct {
	Nodes []string         `json:"nodes,omitempty"`
	Edges []ConstraintEdge `json:"edges,omitempty"`
}

type ConstraintEdge struct {
	From     string `json:"from"`
	To       string `json:"to"`
	Relation string `json:"relation"`
}

type Lineage struct {
	IntentGeneration    string `json:"intent_generation"`
	PolicyGeneration    string `json:"policy_generation"`
	IntentDigest        string `json:"intent_digest"`
	PolicyDigest        string `json:"policy_digest"`
	InputSnapshotDigest string `json:"input_snapshot_digest,omitempty"`
	CompilerVersion     string `json:"compiler_version"`
}

type ReleaseSet struct {
	SchemaVersion string               `json:"schema_version"`
	Generation    string               `json:"generation"`
	Scope         string               `json:"scope"`
	ArtifactIDs   []string             `json:"artifact_ids"`
	ArtifactKinds []string             `json:"artifact_kinds"`
	Dependencies  []ArtifactDependency `json:"dependencies"`
	Lineage       Lineage              `json:"lineage"`
}

type ArtifactDependency struct {
	From     string `json:"from"`
	To       string `json:"to"`
	Relation string `json:"relation"`
}

type CompileRequest struct {
	Intent          PlatformIntent
	Policy          PolicySnapshot
	RuntimeSnapshot RuntimeSnapshot
	// InputSnapshot is retained as a wire compatibility fallback. New callers
	// should use RuntimeSnapshot so generation binding is explicit.
	InputSnapshot map[string]any
	CreatedAt     time.Time
}

// RuntimeSnapshot is the immutable runtime fact view used by one compiler
// invocation. It is input data, never serving configuration.
type RuntimeSnapshot struct {
	DNSPlacements    []DNSPlacementObservation `json:"dns_placements,omitempty"`
	DNSFlatten       []DNSFlattenObservation   `json:"dns_flatten,omitempty"`
	IntentGeneration string                    `json:"intent_generation"`
	PolicyGeneration string                    `json:"policy_generation"`
	Facts            map[string]any            `json:"facts,omitempty"`
	CapturedAt       *time.Time                `json:"captured_at,omitempty"`
	Origins          []OriginObservation       `json:"origins,omitempty"`
	Releases         []ReleaseObservation      `json:"releases,omitempty"`
}

// ReleaseObservation is a fixed runtime fact used to resolve a desired
// traffic policy. Release identity and weights stay in PolicySnapshot;
// readiness and concrete upstream addresses stay here.
type ReleaseObservation struct {
	ObservedAt           time.Time `json:"observed_at"`
	TenantID             string    `json:"tenant_id"`
	StatusReason         string    `json:"status_reason,omitempty"`
	ID                   string    `json:"id"`
	AppID                string    `json:"app_id"`
	Status               string    `json:"status"`
	UpstreamURL          string    `json:"upstream_url"`
	RuntimeID            string    `json:"runtime_id,omitempty"`
	DeploymentGeneration string    `json:"deployment_generation,omitempty"`
}

type CompileResult struct {
	IntentArtifact  model.PlatformArtifact
	PolicyArtifact  model.PlatformArtifact
	RouteArtifact   model.PlatformArtifact
	DNSArtifact     model.PlatformArtifact
	TLSArtifact     model.PlatformArtifact
	ReleaseSet      ReleaseSet
	ReleaseArtifact model.PlatformArtifact
	Lineage         Lineage
}

func BuildReleaseSetArtifact(set ReleaseSet, artifactIDs []string, now time.Time) model.PlatformArtifact {
	set.ArtifactIDs = append([]string(nil), artifactIDs...)
	if now.IsZero() {
		now = time.Now().UTC()
	}
	return buildArtifact(model.PlatformArtifactKindReleaseSet, set.Scope, set.Generation, set, lineageMetadata(set.Lineage), now)
}

func Compile(req CompileRequest) (CompileResult, error) {
	intent := normalizeIntent(req.Intent)
	policy := normalizePolicy(req.Policy)
	if err := validateIntent(intent); err != nil {
		return CompileResult{}, err
	}
	if err := validatePolicy(policy); err != nil {
		return CompileResult{}, err
	}
	intentDigest, err := Digest(intent)
	if err != nil {
		return CompileResult{}, fmt.Errorf("digest platform intent: %w", err)
	}
	policyDigest, err := Digest(policy)
	if err != nil {
		return CompileResult{}, fmt.Errorf("digest policy snapshot: %w", err)
	}
	runtimeSnapshot := req.RuntimeSnapshot
	if runtimeSnapshot.IntentGeneration == "" && runtimeSnapshot.PolicyGeneration == "" && runtimeSnapshot.Facts == nil && runtimeSnapshot.CapturedAt == nil && len(runtimeSnapshot.Origins) == 0 && len(runtimeSnapshot.Releases) == 0 && len(runtimeSnapshot.DNSFlatten) == 0 && len(runtimeSnapshot.DNSPlacements) == 0 && req.InputSnapshot != nil {
		runtimeSnapshot = RuntimeSnapshot{IntentGeneration: intent.Generation, PolicyGeneration: policy.Generation, Facts: req.InputSnapshot}
	}
	if runtimeSnapshot.IntentGeneration == "" {
		runtimeSnapshot.IntentGeneration = intent.Generation
	}
	if runtimeSnapshot.PolicyGeneration == "" {
		runtimeSnapshot.PolicyGeneration = policy.Generation
	}
	if runtimeSnapshot.IntentGeneration != intent.Generation || runtimeSnapshot.PolicyGeneration != policy.Generation {
		return CompileResult{}, fmt.Errorf("runtime snapshot generations must match intent and policy")
	}
	runtimeSnapshot.Origins = append([]OriginObservation(nil), runtimeSnapshot.Origins...)
	runtimeSnapshot.Releases = append([]ReleaseObservation(nil), runtimeSnapshot.Releases...)
	sort.Slice(runtimeSnapshot.Origins, func(i, j int) bool { return runtimeSnapshot.Origins[i].Ref < runtimeSnapshot.Origins[j].Ref })
	sort.Slice(runtimeSnapshot.Releases, func(i, j int) bool { return runtimeSnapshot.Releases[i].ID < runtimeSnapshot.Releases[j].ID })
	runtimeSnapshot.DNSFlatten = normalizeDNSFlattenObservations(runtimeSnapshot.DNSFlatten)
	runtimeSnapshot.DNSPlacements = normalizeDNSPlacementObservations(runtimeSnapshot.DNSPlacements)
	compiledRoutes, err := ResolveRouteOrigins(intent.Routes, runtimeSnapshot, policy)
	if err != nil {
		return CompileResult{}, err
	}
	compiledRoutes, err = ApplyRoutePolicyConstraints(compiledRoutes, policy, runtimeSnapshot.CapturedAt)
	if err != nil {
		return CompileResult{}, err
	}
	compiledRoutes, err = ApplyTrafficPolicyConstraints(compiledRoutes, policy, runtimeSnapshot)
	if err != nil {
		return CompileResult{}, err
	}
	compiledDNS, err := ResolveDNSPlacements(intent, compiledRoutes, runtimeSnapshot, policy)
	if err != nil {
		return CompileResult{}, err
	}
	compiledDNS, err = ResolveDNSFlatten(compiledDNS, runtimeSnapshot, policy)
	if err != nil {
		return CompileResult{}, err
	}
	compiledDNS, err = CompileACMEChallenges(compiledDNS, intent.ACMEChallenges, runtimeSnapshot.CapturedAt)
	if err != nil {
		return CompileResult{}, err
	}
	snapshotDigest := ""
	if runtimeSnapshot.Facts != nil || runtimeSnapshot.IntentGeneration != "" || runtimeSnapshot.PolicyGeneration != "" {
		snapshotDigest, err = Digest(runtimeSnapshot)
		if err != nil {
			return CompileResult{}, fmt.Errorf("digest compiler input snapshot: %w", err)
		}
	}
	lineage := Lineage{
		IntentGeneration:    intent.Generation,
		PolicyGeneration:    policy.Generation,
		IntentDigest:        intentDigest,
		PolicyDigest:        policyDigest,
		InputSnapshotDigest: snapshotDigest,
		CompilerVersion:     CompilerVersion,
	}
	if intent.Scope != policy.Scope {
		return CompileResult{}, fmt.Errorf("intent and policy scopes must match")
	}
	configurationDigest, err := Digest(lineage)
	if err != nil {
		return CompileResult{}, fmt.Errorf("digest configuration lineage: %w", err)
	}
	configurationGeneration := strings.TrimPrefix(configurationDigest, "sha256:")
	now := req.CreatedAt.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}

	routePayload := map[string]any{
		"schema_version": SchemaVersion,
		"generation":     intent.Generation,
		"routes":         compiledRoutes,
		"policy":         policy,
		"lineage":        lineage,
	}
	if len(intent.CachePolicies) > 0 {
		routePayload["cache_policies"] = intent.CachePolicies
	}
	dnsPayload := map[string]any{
		"schema_version": SchemaVersion,
		"generation":     intent.Generation,
		"records":        compiledDNS,
		"policy":         policy,
		"lineage":        lineage,
	}
	tlsPayload := map[string]any{
		"schema_version": SchemaVersion,
		"generation":     intent.Generation,
		"certificates":   intent.TLS,
		"policy":         policy,
		"lineage":        lineage,
	}
	metadata := lineageMetadata(lineage)
	releaseSetGeneration := "release-" + configurationGeneration
	metadata["release_set_generation"] = releaseSetGeneration
	intentArtifact := buildArtifact(model.PlatformArtifactKindPlatformIntent, intent.Scope, intent.Generation, intent, map[string]string{"intent_digest": intentDigest}, now)
	policyArtifact := buildArtifact(model.PlatformArtifactKindPolicySnapshot, policy.Scope, policy.Generation, policy, map[string]string{"policy_digest": policyDigest}, now)
	routeArtifact := buildArtifact(model.PlatformArtifactKindEdgeRouteBundle, intent.Scope, "route-"+configurationGeneration, routePayload, metadata, now)
	dnsArtifact := buildArtifact(model.PlatformArtifactKindDNSAnswerBundle, intent.Scope, "dns-"+configurationGeneration, dnsPayload, metadata, now)
	tlsArtifact := buildArtifact(model.PlatformArtifactKindCaddyRouteConfig, intent.Scope, "tls-"+configurationGeneration, tlsPayload, metadata, now)

	releaseSet := ReleaseSet{
		SchemaVersion: SchemaVersion,
		Generation:    releaseSetGeneration,
		Scope:         firstNonEmpty(intent.Scope, GlobalScopeKey),
		ArtifactKinds: []string{
			model.PlatformArtifactKindEdgeRouteBundle,
			model.PlatformArtifactKindDNSAnswerBundle,
			model.PlatformArtifactKindCaddyRouteConfig,
		},
		Dependencies: []ArtifactDependency{
			{From: model.PlatformArtifactKindDNSAnswerBundle, To: model.PlatformArtifactKindEdgeRouteBundle, Relation: "requires"},
			{From: model.PlatformArtifactKindCaddyRouteConfig, To: model.PlatformArtifactKindEdgeRouteBundle, Relation: "requires"},
		},
		Lineage: lineage,
	}
	releaseSet.ArtifactIDs = []string{routeArtifact.ID, dnsArtifact.ID, tlsArtifact.ID}
	releaseArtifact := buildArtifact(model.PlatformArtifactKindReleaseSet, intent.Scope, releaseSet.Generation, releaseSet, metadata, now)
	return CompileResult{
		IntentArtifact:  intentArtifact,
		PolicyArtifact:  policyArtifact,
		RouteArtifact:   routeArtifact,
		DNSArtifact:     dnsArtifact,
		TLSArtifact:     tlsArtifact,
		ReleaseSet:      releaseSet,
		ReleaseArtifact: releaseArtifact,
		Lineage:         lineage,
	}, nil
}

func normalizeIntent(in PlatformIntent) PlatformIntent {
	out := in
	out.SchemaVersion = firstNonEmpty(strings.TrimSpace(in.SchemaVersion), SchemaVersion)
	out.Scope = firstNonEmpty(strings.TrimSpace(in.Scope), GlobalScopeKey)
	out.Routes = append([]RouteIntent(nil), in.Routes...)
	out.ACMEChallenges = append([]ACMEChallengeIntent(nil), in.ACMEChallenges...)
	sort.Slice(out.ACMEChallenges, func(i, j int) bool { return out.ACMEChallenges[i].ID < out.ACMEChallenges[j].ID })
	out.DNS = append([]DNSIntent(nil), in.DNS...)
	out.TLS = append([]TLSIntent(nil), in.TLS...)
	out.CachePolicies = CloneCachePolicies(in.CachePolicies)
	sort.Slice(out.CachePolicies, func(i, j int) bool { return out.CachePolicies[i].ID < out.CachePolicies[j].ID })
	for i := range out.Routes {
		if out.Routes[i].Streaming != nil {
			value := *out.Routes[i].Streaming
			out.Routes[i].Streaming = &value
		}
		out.Routes[i].Upstreams = append([]UpstreamIntent(nil), out.Routes[i].Upstreams...)
		out.Routes[i].RequestBodyPolicies = model.CloneEdgeRequestBodyPolicies(out.Routes[i].RequestBodyPolicies)
	}
	sort.Slice(out.Routes, func(i, j int) bool {
		if out.Routes[i].Hostname != out.Routes[j].Hostname {
			return out.Routes[i].Hostname < out.Routes[j].Hostname
		}
		return model.NormalizeAppRoutePathPrefix(out.Routes[i].PathPrefix) < model.NormalizeAppRoutePathPrefix(out.Routes[j].PathPrefix)
	})
	sort.Slice(out.TLS, func(i, j int) bool { return out.TLS[i].Hostname < out.TLS[j].Hostname })
	for i := range out.DNS {
		if out.DNS[i].Route != nil {
			value := *out.DNS[i].Route
			value.Hostnames = append([]string(nil), value.Hostnames...)
			for j := range value.Hostnames {
				value.Hostnames[j] = normalizedImportHostname(value.Hostnames[j])
			}
			sort.Strings(value.Hostnames)
			value.Bindings = append([]DNSRouteBinding(nil), value.Bindings...)
			for j := range value.Bindings {
				value.Bindings[j].Hostname = normalizedImportHostname(value.Bindings[j].Hostname)
				value.Bindings[j].PathPrefix = model.NormalizeAppRoutePathPrefix(value.Bindings[j].PathPrefix)
			}
			sort.Slice(value.Bindings, func(a, b int) bool {
				x, y := value.Bindings[a], value.Bindings[b]
				return x.Hostname+"\x00"+x.PathPrefix+"\x00"+x.AppID < y.Hostname+"\x00"+y.PathPrefix+"\x00"+y.AppID
			})
			out.DNS[i].Route = &value
		}
		if out.DNS[i].Application != nil {
			value := *out.DNS[i].Application
			out.DNS[i].Application = &value
		}
		if out.DNS[i].ValueExpirations != nil {
			out.DNS[i].ValueExpirations = cloneDNSExpirations(out.DNS[i].ValueExpirations)
		}
		if out.DNS[i].Flatten != nil {
			value := *out.DNS[i].Flatten
			out.DNS[i].Flatten = &value
		}
		out.DNS[i].Hostname = normalizedImportHostname(out.DNS[i].Hostname)
		out.DNS[i].Type = strings.ToUpper(strings.TrimSpace(out.DNS[i].Type))
		out.DNS[i].Values = normalizeDNSValues(out.DNS[i].Type, out.DNS[i].Values)
		if out.DNS[i].Type == "FUGUE_ROUTE" && out.DNS[i].Values == nil {
			out.DNS[i].Values = []string{}
		}
	}
	sort.Slice(out.DNS, func(i, j int) bool {
		return out.DNS[i].Hostname+"\x00"+out.DNS[i].Type < out.DNS[j].Hostname+"\x00"+out.DNS[j].Type
	})
	return out
}

func normalizePolicy(in PolicySnapshot) PolicySnapshot {
	out := in
	out.SchemaVersion = firstNonEmpty(strings.TrimSpace(in.SchemaVersion), SchemaVersion)
	out.Scope = firstNonEmpty(strings.TrimSpace(in.Scope), GlobalScopeKey)
	if out.MinimumHealthyEdges <= 0 {
		out.MinimumHealthyEdges = 1
	}
	if out.MaxStaleSeconds <= 0 {
		out.MaxStaleSeconds = 86400
	}
	out.CanaryWeights = append([]int(nil), in.CanaryWeights...)
	out.DependencyOrder = append([]string(nil), in.DependencyOrder...)
	out.ConstraintGraph = normalizeConstraintGraph(in.ConstraintGraph)
	out.RouteConstraints = append([]RoutePolicyConstraint(nil), in.RouteConstraints...)
	for i := range out.RouteConstraints {
		out.RouteConstraints[i].ExcludedEdgeIDs = uniqueSorted(in.RouteConstraints[i].ExcludedEdgeIDs)
		out.RouteConstraints[i].ExcludedEdgeGroupIDs = uniqueSorted(in.RouteConstraints[i].ExcludedEdgeGroupIDs)
		if in.RouteConstraints[i].ExclusionExpiresAt != nil {
			value := *in.RouteConstraints[i].ExclusionExpiresAt
			out.RouteConstraints[i].ExclusionExpiresAt = &value
		}
	}
	out.TrafficConstraints = append([]TrafficPolicyConstraint(nil), in.TrafficConstraints...)
	sort.Slice(out.RouteConstraints, func(i, j int) bool { return out.RouteConstraints[i].Hostname < out.RouteConstraints[j].Hostname })
	sort.Slice(out.TrafficConstraints, func(i, j int) bool { return out.TrafficConstraints[i].AppID < out.TrafficConstraints[j].AppID })
	return out
}

func validateIntent(in PlatformIntent) error {
	if in.SchemaVersion != SchemaVersion || strings.TrimSpace(in.Generation) == "" {
		return fmt.Errorf("platform intent requires schema_version %q and generation", SchemaVersion)
	}
	seen := map[string]struct{}{}
	for _, route := range in.Routes {
		if strings.TrimSpace(route.Hostname) == "" || strings.TrimSpace(route.UpstreamURL) == "" {
			return fmt.Errorf("route intent requires hostname and upstream_url")
		}
		path := model.NormalizeAppRoutePathPrefix(route.PathPrefix)
		if route.PathPrefix != "" && route.PathPrefix != path {
			return fmt.Errorf("route intent requires a canonical path_prefix")
		}
		if route.ServicePort < 0 || route.ServicePort > 65535 {
			return fmt.Errorf("route intent service_port is outside 0..65535")
		}
		if route.OriginRef != "" && (route.OriginRef != strings.TrimSpace(route.OriginRef) || strings.TrimSpace(route.RuntimeID) == "") {
			return fmt.Errorf("route origin_ref requires a canonical reference and explicit runtime_id")
		}
		key := strings.Trim(strings.ToLower(strings.TrimSpace(route.Hostname)), ".") + "\x00" + path
		if _, ok := seen[key]; ok {
			return fmt.Errorf("duplicate route hostname/path %q %q", route.Hostname, path)
		}
		seen[key] = struct{}{}
		if err := ValidateUpstreamIntents(route.Upstreams); err != nil {
			return err
		}
	}
	if err := validateDNSConfiguration(in.DNS); err != nil {
		return err
	}
	if err := validateDNSApplicationOwners(in.DNS, in.Routes); err != nil {
		return err
	}
	if err := ValidateACMEChallenges(in.ACMEChallenges); err != nil {
		return err
	}
	return ValidateRouteBehavior(in.Routes, in.CachePolicies)
}

// ValidatePlatformIntent validates a normalized, strongly typed platform
// intent for callers outside the compiler (for example artifact gates).
func ValidatePlatformIntent(in PlatformIntent) error {
	return validateIntent(normalizeIntent(in))
}

// NormalizePlatformIntent returns the canonical representation used for
// artifact digests and compiler replay.
func NormalizePlatformIntent(in PlatformIntent) PlatformIntent { return normalizeIntent(in) }

// PlatformIntentGeneration returns the deterministic generation for a
// canonical intent. Runtime observations are excluded from this digest.
func PlatformIntentGeneration(in PlatformIntent) (string, error) {
	canonical := NormalizePlatformIntent(in)
	canonical.Generation = ""
	canonical.CreatedAt = time.Time{}
	digest, err := Digest(canonical)
	if err != nil {
		return "", err
	}
	return "intent_" + strings.TrimPrefix(digest, "sha256:"), nil
}

// PolicySnapshotGeneration returns a stable identifier for policy content.
// Version metadata and creation time are excluded so runtime observation
// refreshes cannot create a new policy generation.
func PolicySnapshotGeneration(in PolicySnapshot) (string, error) {
	canonical := NormalizePolicySnapshot(in)
	canonical.Generation = ""
	canonical.CreatedAt = time.Time{}
	digest, err := Digest(canonical)
	if err != nil {
		return "", err
	}
	return "policy_" + strings.TrimPrefix(digest, "sha256:"), nil
}

func validatePolicy(in PolicySnapshot) error {
	if in.SchemaVersion != SchemaVersion || strings.TrimSpace(in.Generation) == "" {
		return fmt.Errorf("policy snapshot requires schema_version %q and generation", SchemaVersion)
	}
	if in.MinimumHealthyEdges < 1 || in.MaxStaleSeconds < 1 {
		return fmt.Errorf("policy snapshot bounds are invalid")
	}
	for _, weight := range in.CanaryWeights {
		if weight < 0 || weight > 100 {
			return fmt.Errorf("canary weight %d is outside 0..100", weight)
		}
	}
	if err := validateDependencyOrder(in.DependencyOrder); err != nil {
		return err
	}
	if err := validateConstraintGraph(in.ConstraintGraph); err != nil {
		return err
	}
	return validatePolicyRules(in)
}

// ValidatePolicySnapshot validates a normalized, strongly typed policy
// snapshot for callers outside the compiler (for example artifact gates).
func ValidatePolicySnapshot(in PolicySnapshot) error {
	return validatePolicy(normalizePolicy(in))
}

// NormalizePolicySnapshot returns the canonical representation used for
// artifact digests and compiler replay.
func NormalizePolicySnapshot(in PolicySnapshot) PolicySnapshot { return normalizePolicy(in) }

func normalizeConstraintGraph(graph ConstraintGraph) ConstraintGraph {
	out := ConstraintGraph{Nodes: append([]string(nil), graph.Nodes...), Edges: append([]ConstraintEdge(nil), graph.Edges...)}
	for i := range out.Nodes {
		out.Nodes[i] = strings.TrimSpace(out.Nodes[i])
	}
	for i := range out.Edges {
		out.Edges[i].From = strings.TrimSpace(out.Edges[i].From)
		out.Edges[i].To = strings.TrimSpace(out.Edges[i].To)
		out.Edges[i].Relation = strings.TrimSpace(strings.ToLower(out.Edges[i].Relation))
	}
	sort.Strings(out.Nodes)
	sort.Slice(out.Edges, func(i, j int) bool {
		if out.Edges[i].From != out.Edges[j].From {
			return out.Edges[i].From < out.Edges[j].From
		}
		if out.Edges[i].To != out.Edges[j].To {
			return out.Edges[i].To < out.Edges[j].To
		}
		return out.Edges[i].Relation < out.Edges[j].Relation
	})
	return out
}

func validateConstraintGraph(graph ConstraintGraph) error {
	allowed := map[string]bool{"requires": true, "blocks": true, "before": true, "produces": true, "rollback_to": true}
	nodes := make(map[string]struct{}, len(graph.Nodes))
	for _, node := range graph.Nodes {
		if node == "" {
			return fmt.Errorf("constraint graph contains an empty node")
		}
		if _, exists := nodes[node]; exists {
			return fmt.Errorf("constraint graph contains duplicate node %q", node)
		}
		nodes[node] = struct{}{}
	}
	seenEdges := map[string]struct{}{}
	adjacency := make(map[string][]string, len(nodes))
	for _, edge := range graph.Edges {
		if _, ok := nodes[edge.From]; !ok {
			return fmt.Errorf("constraint graph edge references unknown node %q", edge.From)
		}
		if _, ok := nodes[edge.To]; !ok {
			return fmt.Errorf("constraint graph edge references unknown node %q", edge.To)
		}
		if !allowed[edge.Relation] {
			return fmt.Errorf("constraint graph relation %q is unsupported", edge.Relation)
		}
		key := edge.From + "\x00" + edge.To + "\x00" + edge.Relation
		if _, exists := seenEdges[key]; exists {
			return fmt.Errorf("constraint graph contains duplicate edge %q -> %q", edge.From, edge.To)
		}
		seenEdges[key] = struct{}{}
		if edge.Relation != "rollback_to" {
			adjacency[edge.From] = append(adjacency[edge.From], edge.To)
		}
	}
	state := make(map[string]uint8, len(nodes))
	var visit func(string) error
	visit = func(node string) error {
		switch state[node] {
		case 1:
			return fmt.Errorf("constraint graph contains a cycle at %q", node)
		case 2:
			return nil
		}
		state[node] = 1
		for _, next := range adjacency[node] {
			if err := visit(next); err != nil {
				return err
			}
		}
		state[node] = 2
		return nil
	}
	for node := range nodes {
		if err := visit(node); err != nil {
			return err
		}
	}
	return nil
}

func validateDependencyOrder(nodes []string) error {
	seen := map[string]struct{}{}
	for _, node := range nodes {
		node = strings.TrimSpace(node)
		if node == "" {
			return fmt.Errorf("dependency order contains an empty node")
		}
		if _, ok := seen[node]; ok {
			return fmt.Errorf("dependency order contains duplicate node %q", node)
		}
		seen[node] = struct{}{}
	}
	return nil
}

func Digest(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func LineageFromArtifact(artifact model.PlatformArtifact) Lineage {
	return Lineage{
		IntentGeneration:    artifact.Metadata["intent_generation"],
		PolicyGeneration:    artifact.Metadata["policy_generation"],
		IntentDigest:        artifact.Metadata["intent_digest"],
		PolicyDigest:        artifact.Metadata["policy_digest"],
		InputSnapshotDigest: artifact.Metadata["input_snapshot_digest"],
		CompilerVersion:     artifact.Metadata["compiler_version"],
	}
}

func LineageMetadata(lineage Lineage) map[string]string {
	return lineageMetadata(lineage)
}

func buildArtifact(kind, scope, generation string, content any, metadata map[string]string, now time.Time) model.PlatformArtifact {
	var payload map[string]any
	raw, _ := json.Marshal(content)
	_ = json.Unmarshal(raw, &payload)
	return model.PlatformArtifact{
		ArtifactKind:  kind,
		Scope:         model.PlatformArtifactScope{ScopeType: "global", Key: scope},
		SchemaVersion: model.PlatformArtifactSchemaVersionV1,
		Generation:    generation,
		Content:       payload,
		Metadata:      metadata,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
}

func lineageMetadata(lineage Lineage) map[string]string {
	return map[string]string{
		"intent_generation":     lineage.IntentGeneration,
		"policy_generation":     lineage.PolicyGeneration,
		"intent_digest":         lineage.IntentDigest,
		"policy_digest":         lineage.PolicyDigest,
		"input_snapshot_digest": lineage.InputSnapshotDigest,
		"compiler_version":      lineage.CompilerVersion,
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func uniqueSorted(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
