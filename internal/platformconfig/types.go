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
	CompilerVersion = "platform-config-compiler/v1"
	GlobalScopeKey  = "global"
)

// PlatformIntent is the versioned description of what Fugue should serve.
// It intentionally contains no runtime health, ACK, or observed state.
type PlatformIntent struct {
	SchemaVersion string        `json:"schema_version"`
	Generation    string        `json:"generation"`
	Scope         string        `json:"scope"`
	Routes        []RouteIntent `json:"routes,omitempty"`
	DNS           []DNSIntent   `json:"dns,omitempty"`
	TLS           []TLSIntent   `json:"tls,omitempty"`
	CreatedAt     time.Time     `json:"created_at,omitempty"`
}

type RouteIntent struct {
	Hostname    string `json:"hostname"`
	UpstreamURL string `json:"upstream_url"`
	Enabled     bool   `json:"enabled"`
	EdgeGroupID string `json:"edge_group_id,omitempty"`
}

type DNSIntent struct {
	Hostname string   `json:"hostname"`
	Type     string   `json:"type"`
	Values   []string `json:"values"`
	TTL      int      `json:"ttl"`
}

type TLSIntent struct {
	Hostname string `json:"hostname"`
	Policy   string `json:"policy"`
}

// PolicySnapshot contains changeable release constraints. It is deliberately
// typed and bounded; it is not an arbitrary executable policy language.
type PolicySnapshot struct {
	SchemaVersion       string          `json:"schema_version"`
	Generation          string          `json:"generation"`
	Scope               string          `json:"scope"`
	RequireTLSReady     bool            `json:"require_tls_ready"`
	RequireRouteReady   bool            `json:"require_route_ready"`
	MinimumHealthyEdges int             `json:"minimum_healthy_edges"`
	MaxStaleSeconds     int             `json:"max_stale_seconds"`
	CanaryWeights       []int           `json:"canary_weights,omitempty"`
	DependencyOrder     []string        `json:"dependency_order,omitempty"`
	ConstraintGraph     ConstraintGraph `json:"constraint_graph,omitempty"`
	CreatedAt           time.Time       `json:"created_at,omitempty"`
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
	IntentDigest        string `json:"intent_digest"`
	PolicyDigest        string `json:"policy_digest"`
	InputSnapshotDigest string `json:"input_snapshot_digest,omitempty"`
	CompilerVersion     string `json:"compiler_version"`
}

type ReleaseSet struct {
	SchemaVersion string   `json:"schema_version"`
	Generation    string   `json:"generation"`
	Scope         string   `json:"scope"`
	ArtifactIDs   []string `json:"artifact_ids"`
	ArtifactKinds []string `json:"artifact_kinds"`
	Lineage       Lineage  `json:"lineage"`
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
	IntentGeneration string         `json:"intent_generation"`
	PolicyGeneration string         `json:"policy_generation"`
	Facts            map[string]any `json:"facts,omitempty"`
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
	if runtimeSnapshot.IntentGeneration == "" && runtimeSnapshot.PolicyGeneration == "" && runtimeSnapshot.Facts == nil && req.InputSnapshot != nil {
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
	snapshotDigest := ""
	if runtimeSnapshot.Facts != nil || runtimeSnapshot.IntentGeneration != "" || runtimeSnapshot.PolicyGeneration != "" {
		snapshotDigest, err = Digest(runtimeSnapshot)
		if err != nil {
			return CompileResult{}, fmt.Errorf("digest compiler input snapshot: %w", err)
		}
	}
	lineage := Lineage{
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
		"routes":         intent.Routes,
		"policy":         policy,
		"lineage":        lineage,
	}
	dnsPayload := map[string]any{
		"schema_version": SchemaVersion,
		"generation":     intent.Generation,
		"records":        intent.DNS,
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
	out.DNS = append([]DNSIntent(nil), in.DNS...)
	out.TLS = append([]TLSIntent(nil), in.TLS...)
	sort.Slice(out.Routes, func(i, j int) bool { return out.Routes[i].Hostname < out.Routes[j].Hostname })
	sort.Slice(out.DNS, func(i, j int) bool { return out.DNS[i].Hostname < out.DNS[j].Hostname })
	sort.Slice(out.TLS, func(i, j int) bool { return out.TLS[i].Hostname < out.TLS[j].Hostname })
	for i := range out.DNS {
		out.DNS[i].Type = strings.ToUpper(strings.TrimSpace(out.DNS[i].Type))
		out.DNS[i].Values = uniqueSorted(out.DNS[i].Values)
	}
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
		if _, ok := seen[route.Hostname]; ok {
			return fmt.Errorf("duplicate route hostname %q", route.Hostname)
		}
		seen[route.Hostname] = struct{}{}
	}
	return nil
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
	return validateConstraintGraph(in.ConstraintGraph)
}

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
