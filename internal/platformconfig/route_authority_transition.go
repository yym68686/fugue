package platformconfig

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"fugue/internal/edgetopology"
	"fugue/internal/model"
	"fugue/internal/routebinding"
	"fugue/internal/routeproof"
	"fugue/internal/trafficbinding"
)

// PreviousTrafficPublicationReference pins a complete global publication. Its
// DNS child supplies signed physical endpoint ownership, independently of the
// neutral Cells' declared membership. Only a selected full release is eligible.
type PreviousTrafficPublicationReference struct {
	ReleaseSetID        string `json:"release_set_id"`
	ReleaseSetDigest    string `json:"release_set_digest"`
	ReleaseID           string `json:"release_id"`
	FencingToken        int64  `json:"fencing_token"`
	RouteArtifactID     string `json:"route_artifact_id"`
	RouteArtifactDigest string `json:"route_artifact_digest"`
	TLSArtifactID       string `json:"tls_artifact_id"`
	TLSArtifactDigest   string `json:"tls_artifact_digest"`
	DNSArtifactID       string `json:"dns_artifact_id"`
	DNSArtifactDigest   string `json:"dns_artifact_digest"`
}

type PreviousTrafficPublicationInput struct {
	ProducerPolicy *model.PlatformPublicationPrecondition `json:"producer_policy,omitempty"`
	Reference      PreviousTrafficPublicationReference    `json:"reference"`
	Parent         model.PlatformArtifact                 `json:"parent"`
	Route          model.PlatformArtifact                 `json:"route"`
	TLS            model.PlatformArtifact                 `json:"tls"`
	DNS            model.PlatformArtifact                 `json:"dns"`
}

type RouteAuthorityTransition struct {
	ReferenceMode       string                              `json:"reference_mode,omitempty"`
	PreviousTopology    edgetopology.Intent                 `json:"previous_topology"`
	PreviousPublication PreviousTrafficPublicationReference `json:"previous_publication"`
}

const DNSRetainedGlobalReferenceV1 = "retained-global-v1"

func DNSUsesRetainedRoutes(intent PlatformIntent) bool {
	return intent.RouteAuthorityTransition != nil && intent.RouteAuthorityTransition.ReferenceMode == DNSRetainedGlobalReferenceV1
}

func (r PreviousTrafficPublicationReference) Validate() error {
	if r.FencingToken <= 0 {
		return fmt.Errorf("previous traffic publication requires a positive full fence")
	}
	ids := []string{r.ReleaseSetID, r.RouteArtifactID, r.TLSArtifactID, r.DNSArtifactID}
	seen := map[string]bool{}
	for _, id := range append(ids, r.ReleaseID) {
		if !validDNSConsumerIdentity(id) || strings.ContainsAny(id, " /") || seen[id] {
			return fmt.Errorf("previous traffic publication identities invalid")
		}
		seen[id] = true
	}
	for _, digest := range []string{r.ReleaseSetDigest, r.RouteArtifactDigest, r.TLSArtifactDigest, r.DNSArtifactDigest} {
		if !trafficConsumerDigest.MatchString(digest) {
			return fmt.Errorf("previous traffic publication digest invalid")
		}
	}
	return nil
}

// RouteAuthorityAliases changes only explicitly declared aliases. It cannot
// move a machine between cells or change its pools, capabilities or risk labels.
func RouteAuthorityAliases(intent PlatformIntent) (map[string]string, error) {
	t := intent.RouteAuthorityTransition
	if t == nil {
		return nil, nil
	}
	if t.ReferenceMode != "" && t.ReferenceMode != DNSRetainedGlobalReferenceV1 {
		return nil, fmt.Errorf("unsupported DNS routing reference mode")
	}
	if DNSUsesRetainedRoutes(intent) && len(intent.CellRoutePublications) != 0 {
		return nil, fmt.Errorf("retained DNS cannot mix replacement routing publications")
	}
	if intent.PublicationRole != PublicationRoleCellDNS || intent.EdgeTopology == nil || intent.EdgeTopology.Validate() != nil || t.PreviousTopology.Validate() != nil || t.PreviousPublication.Validate() != nil {
		return nil, fmt.Errorf("DNS authority transition requires exact valid publication and topology")
	}
	expected := t.PreviousTopology.Clone()
	aliases := map[string]string{}
	for i, c := range expected.Cells {
		if c.LegacyGroupID != "" {
			if aliases[c.LegacyGroupID] != "" {
				return nil, fmt.Errorf("duplicate previous authority alias")
			}
			aliases[c.LegacyGroupID] = c.ID
			expected.Cells[i].LegacyGroupID = ""
		}
	}
	if len(aliases) == 0 || !reflect.DeepEqual(expected, intent.EdgeTopology.Clone()) {
		return nil, fmt.Errorf("authority transition changes physical membership, pools or failure domains")
	}
	return aliases, nil
}

func ValidatePreviousTrafficPublication(p PreviousTrafficPublicationInput) error {
	key, cacheable := dnsPreviousValidationKey(p)
	if cacheable && dnsPreviousValidations.contains(key) {
		return nil
	}
	err := validatePreviousTrafficPublication(p)
	if err == nil && cacheable {
		dnsPreviousValidations.remember(key)
	}
	return err
}

func validatePreviousTrafficPublication(p PreviousTrafficPublicationInput) error {
	r := p.Reference
	if err := r.Validate(); err != nil {
		return err
	}
	if _, err := ValidateReleaseComposition(p.Parent); err != nil {
		return err
	}
	var set ReleaseSet
	raw, _ := json.Marshal(p.Parent.Content)
	if json.Unmarshal(raw, &set) != nil || set.PublicationRole != "" || set.Scope != "global" || set.Scope != p.Parent.ScopeKey || set.Generation != p.Parent.Generation || !reflect.DeepEqual(set.Lineage, LineageFromArtifact(p.Parent)) {
		return fmt.Errorf("previous publication is not a complete global release")
	}
	artifacts := []model.PlatformArtifact{p.Parent, p.Route, p.TLS, p.DNS}
	ids := []string{r.ReleaseSetID, r.RouteArtifactID, r.TLSArtifactID, r.DNSArtifactID}
	hashes := []string{r.ReleaseSetDigest, r.RouteArtifactDigest, r.TLSArtifactDigest, r.DNSArtifactDigest}
	kinds := []string{model.PlatformArtifactKindReleaseSet, model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindCaddyRouteConfig, model.PlatformArtifactKindDNSAnswerBundle}
	for i, a := range artifacts {
		hash, digestRaw, native, err := dnsValidationContentDigest(a.Content)
		if err != nil || a.ID != ids[i] || a.ContentHash != hashes[i] || hash != a.ContentHash || a.ArtifactKind != kinds[i] || a.Status != model.PlatformArtifactStatusValidated || a.SchemaVersion != model.PlatformArtifactSchemaVersionV1 || a.GenerationSequence <= 0 || a.ScopeKey != "global" || a.Scope.Key != a.ScopeKey {
			return fmt.Errorf("previous publication differs from exact artifact pins")
		}
		if i == 0 {
			continue
		}
		index := slices.Index(set.ArtifactKinds, a.ArtifactKind)
		if index < 0 || set.ArtifactIDs[index] != a.ID || a.Metadata["release_set_generation"] != p.Parent.Generation || !reflect.DeepEqual(set.Lineage, LineageFromArtifact(a)) || ValidateTrafficCohortProjection(p.Parent, a) != nil {
			return fmt.Errorf("previous child is outside the signed composition")
		}
		var payload struct {
			Policy  PolicySnapshot `json:"policy"`
			Lineage Lineage        `json:"lineage"`
		}
		raw, _ := marshalDNSChildValidationFields(a.Content, digestRaw, native)
		if json.Unmarshal(raw, &payload) != nil || !reflect.DeepEqual(payload.Lineage, set.Lineage) || ValidatePolicySnapshot(payload.Policy) != nil {
			return fmt.Errorf("previous policy or lineage invalid")
		}
		digest, err := Digest(payload.Policy)
		if err != nil || digest != set.Lineage.PolicyDigest {
			return fmt.Errorf("previous child policy differs from signed lineage")
		}
	}
	return nil
}

func PreviousTrafficPublicationBinding(p PreviousTrafficPublicationInput, group string) (*model.TrafficReleaseBinding, error) {
	if err := ValidatePreviousTrafficPublication(p); err != nil {
		return nil, err
	}
	projection, err := ProjectRouteArtifact(p.Route)
	if err != nil {
		return nil, err
	}
	parent, route, r := p.Parent, p.Route, p.Reference
	lineage := LineageFromArtifact(parent)
	b := &model.TrafficReleaseBinding{Schema: trafficbinding.Schema, ReleaseSetID: parent.ID, ReleaseSetDigest: parent.ContentHash, ReleaseSetGeneration: parent.Generation, RouteArtifactID: route.ID, RouteArtifactDigest: route.ContentHash, RouteArtifactGeneration: route.Generation, RouteArtifactSequence: route.GenerationSequence, ReleaseID: r.ReleaseID, ReleaseChannel: "full", FencingToken: r.FencingToken, ScopeKey: parent.ScopeKey, IntentDigest: lineage.IntentDigest, PolicyDigest: lineage.PolicyDigest, InputSnapshotDigest: lineage.InputSnapshotDigest, CompilerVersion: lineage.CompilerVersion, ProjectionDigest: trafficbinding.ProjectionDigest(projection)}
	return b, trafficbinding.ValidateGroup(b, group, true)
}

func DecodePreviousTrafficPublication(a model.PlatformArtifact) (*PreviousTrafficPublicationInput, error) {
	raw, err := json.Marshal(a.Content)
	if err != nil {
		return nil, fmt.Errorf("previous traffic publication cannot be decoded")
	}
	return decodePreviousTrafficPublication(raw)
}

func decodePreviousTrafficPublication(raw []byte) (*PreviousTrafficPublicationInput, error) {
	var payload struct {
		Previous *PreviousTrafficPublicationInput `json:"previous_traffic_publication"`
		Source   *CellDNSPlanSource               `json:"cell_dns_source"`
	}
	if json.Unmarshal(raw, &payload) != nil {
		return nil, fmt.Errorf("previous traffic publication cannot be decoded")
	}
	return validateDecodedPreviousTrafficPublication(payload.Previous, payload.Source)
}

func validateDecodedPreviousTrafficPublication(previous *PreviousTrafficPublicationInput, source *CellDNSPlanSource) (*PreviousTrafficPublicationInput, error) {
	declared := source != nil && source.Intent.RouteAuthorityTransition != nil
	if (previous != nil) != declared {
		return nil, fmt.Errorf("previous publication requires explicit signed transition")
	}
	if previous != nil {
		if err := ValidatePreviousTrafficPublication(*previous); err != nil {
			return nil, err
		}
		if previous.Reference != source.Intent.RouteAuthorityTransition.PreviousPublication {
			return nil, fmt.Errorf("previous publication differs from intent")
		}
	}
	return previous, nil
}

// Compare all compiled route behavior, cache policies, TLS authorization and
// hard route policy. Only exact placement aliases are transformed. Diagnostic
// TLS event times remain in their signed sources; fresh TLS proofs are still
// required, and comparison never copies or renews their evidence.
func validateTransitionRouteEquivalence(previous PreviousTrafficPublicationInput, next CellRoutePublicationInput, aliases map[string]string) error {
	oldRoute, err := transitionBehavior(previous.Route.Content, aliases, false)
	if err != nil {
		return err
	}
	newRoute, err := transitionBehavior(next.Route.Content, nil, false)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(oldRoute, newRoute) {
		return fmt.Errorf("authority transition changes compiled route behavior or hard constraints")
	}
	oldTLS, err := transitionBehavior(previous.TLS.Content, aliases, true)
	if err != nil {
		return err
	}
	newTLS, err := transitionBehavior(next.TLS.Content, nil, true)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(oldTLS, newTLS) {
		return fmt.Errorf("authority transition changes TLS authorization or policy")
	}
	return nil
}

func transitionBehavior(content map[string]any, aliases map[string]string, tls bool) (map[string]any, error) {
	raw, err := json.Marshal(content)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if json.Unmarshal(raw, &out) != nil {
		return nil, fmt.Errorf("invalid transition content")
	}
	delete(out, "generation")
	delete(out, "lineage")
	policy, ok := out["policy"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("transition policy missing")
	}
	for _, key := range []string{"generation", "scope", "publication_role", "authority_cell_id", "consumer_topology_digest", "traffic_rollout_cohorts", "dns_placement_mode", "dns_query_policy", "dns_authorities", "dns_client_policies", "dns_answer_rules", "dns_readiness"} {
		delete(policy, key)
	}
	replace := func(row map[string]any, key string) {
		if value, ok := row[key].(string); ok && aliases[value] != "" {
			row[key] = aliases[value]
		}
	}
	replaceList := func(row map[string]any, key string) {
		if values, ok := row[key].([]any); ok {
			for i, v := range values {
				if text, ok := v.(string); ok && aliases[text] != "" {
					values[i] = aliases[text]
				}
			}
			slices.SortFunc(values, func(a, b any) int { return strings.Compare(fmt.Sprint(a), fmt.Sprint(b)) })
		}
	}
	if rules, ok := policy["route_constraints"].([]any); ok {
		for _, value := range rules {
			row, ok := value.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("invalid transition route constraint")
			}
			replace(row, "edge_group_id")
			replaceList(row, "excluded_edge_group_ids")
		}
	}
	if routes, ok := out["routes"].([]any); ok {
		for _, value := range routes {
			row, ok := value.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("invalid transition route")
			}
			replace(row, "dns_placement_edge_group_id")
			replaceList(row, "excluded_edge_group_ids")
		}
	}
	if tls {
		if states, ok := out["domain_states"].([]any); ok {
			for _, value := range states {
				row, ok := value.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("invalid TLS domain state")
				}
				for _, key := range []string{"verified_at", "tls_last_checked_at", "tls_ready_at"} {
					delete(row, key)
				}
			}
		}
	}
	return out, nil
}

func validateRouteAuthorityTransitionInputs(intent PlatformIntent, publications []CellRoutePublicationInput, previous *PreviousTrafficPublicationInput) error {
	if (intent.RouteAuthorityTransition != nil) != (previous != nil) {
		return fmt.Errorf("DNS transition and retained previous publication must be paired")
	}
	if previous == nil {
		return nil
	}
	aliases, err := RouteAuthorityAliases(intent)
	if err != nil {
		return err
	}
	if previous.Reference != intent.RouteAuthorityTransition.PreviousPublication {
		return fmt.Errorf("DNS transition previous publication differs from exact intent")
	}
	if err := ValidatePreviousTrafficPublication(*previous); err != nil {
		return err
	}
	if DNSUsesRetainedRoutes(intent) && len(publications) != 0 {
		return fmt.Errorf("retained DNS cannot accept replacement routing publications")
	}
	for _, next := range publications {
		if err := validateTransitionRouteEquivalence(*previous, next, aliases); err != nil {
			return err
		}
	}
	return nil
}

func addPreviousAuthorityRequirements(plan *DNSReadinessPlan, intent PlatformIntent, previous PreviousTrafficPublicationInput, policy PolicySnapshot) error {
	var payload struct {
		Plan   *DNSReadinessPlan `json:"readiness_plan"`
		Policy PolicySnapshot    `json:"policy"`
	}
	raw, _ := json.Marshal(previous.DNS.Content)
	if json.Unmarshal(raw, &payload) != nil || payload.Plan == nil || payload.Policy.DNSReadiness == nil || ValidateDNSReadinessPlan(payload.Plan, payload.Policy.DNSReadiness) != nil {
		return fmt.Errorf("previous signed DNS readiness plan invalid")
	}
	if policy.DNSReadiness.FactFreshnessSeconds > payload.Policy.DNSReadiness.FactFreshnessSeconds || policy.MaxStaleSeconds > payload.Policy.MaxStaleSeconds {
		return fmt.Errorf("DNS transition weakens previous evidence lifetime")
	}
	aliases, err := RouteAuthorityAliases(intent)
	if err != nil {
		return err
	}
	groups := map[string]string{}
	for old, cell := range aliases {
		groups[cell] = old
	}
	members := map[string]string{}
	for _, edge := range intent.EdgeTopology.Edges {
		members[edge.ID] = edge.AuthorityCellID
	}
	projection, err := ProjectRouteArtifact(previous.Route)
	if err != nil {
		return err
	}
	routes := map[string]model.EdgeRouteIntent{}
	for _, r := range projection.Routes {
		routes[r.Hostname+"\x00"+model.NormalizeAppRoutePathPrefix(r.PathPrefix)] = r
	}
	key := func(p DNSReadinessProbe) string {
		return p.EdgeID + "\x00" + p.Address + "\x00" + p.Hostname + "\x00" + p.Path + "\x00" + p.State
	}
	oldProbes := map[string]DNSReadinessProbe{}
	oldByID := map[string]DNSReadinessProbe{}
	for _, p := range payload.Plan.Probes {
		if p.PreviousAuthority != nil || p.CellPublicationDigest != "" {
			return fmt.Errorf("nested DNS authority transition rejected")
		}
		oldByID[p.ID] = p
		cell := members[p.EdgeID]
		if cell == "" || groups[cell] != p.EdgeGroupID {
			continue
		}
		route, ok := routes[p.Hostname+"\x00"+p.Path]
		if !ok {
			return fmt.Errorf("previous DNS probe has no complete route")
		}
		digest, err := routeproof.Digest(routebinding.FromIntent(route, p.EdgeGroupID))
		if err != nil || digest != p.RouteDigest {
			return fmt.Errorf("previous DNS requirement differs from previous route behavior")
		}
		if _, duplicate := oldProbes[key(p)]; duplicate {
			return fmt.Errorf("ambiguous previous DNS endpoint proof")
		}
		oldProbes[key(p)] = p
	}
	oldRecords := map[string]DNSReadinessRecord{}
	for _, r := range payload.Plan.Records {
		oldRecords[r.Hostname] = r
	}
	refDigest, _ := Digest(previous.Reference)
	ids := map[string]string{}
	newByID := map[string]DNSReadinessProbe{}
	for i, p := range plan.Probes {
		oldID := p.ID
		if old, ok := oldProbes[key(p)]; ok {
			p.PreviousAuthority = &DNSPreviousAuthority{PublicationDigest: refDigest, EdgeGroupID: old.EdgeGroupID, RouteDigest: old.RouteDigest}
			p.FactMaxAgeSeconds = min(DNSReadinessFactMaxAge(p, policy.DNSReadiness), DNSReadinessFactMaxAge(old, payload.Policy.DNSReadiness))
			p.ID, err = DNSReadinessProbeID(p)
			if err != nil {
				return err
			}
		} else {
			return fmt.Errorf("previous DNS has no authorized physical endpoint proof for a transition requirement")
		}
		ids[oldID] = p.ID
		plan.Probes[i] = p
		newByID[p.ID] = p
	}
	for i := range plan.Records {
		record := &plan.Records[i]
		oldRecord, exists := oldRecords[record.Hostname]
		if exists {
			record.MinimumHealthyEdges = max(record.MinimumHealthyEdges, oldRecord.MinimumHealthyEdges)
			record.RequireDualStack = record.RequireDualStack || oldRecord.RequireDualStack
			record.MinDistinctCells = max(record.MinDistinctCells, oldRecord.MinDistinctCells)
			for d, n := range oldRecord.MinDistinctDomains {
				record.MinDistinctDomains[d] = max(record.MinDistinctDomains[d], n)
			}
		}
		for j := range record.Targets {
			target := &record.Targets[j]
			hasAlternative := false
			newKeys := map[string]bool{}
			for k, id := range target.ProbeIDs {
				target.ProbeIDs[k] = ids[id]
				p := newByID[ids[id]]
				hasAlternative = hasAlternative || p.PreviousAuthority != nil
				newKeys[key(p)] = true
			}
			slices.Sort(target.ProbeIDs)
			if !hasAlternative {
				continue
			}
			target.RequireSinglePublication = true
			var oldTarget *DNSReadinessTarget
			for _, candidate := range oldRecord.Targets {
				if candidate.EdgeID == target.EdgeID && candidate.Address == target.Address && candidate.EdgeGroupID == groups[target.EdgeGroupID] && candidate.Family == target.Family {
					copy := candidate
					oldTarget = &copy
				}
			}
			if oldTarget == nil || len(oldTarget.ProbeIDs) != len(target.ProbeIDs) {
				return fmt.Errorf("DNS transition changes whole-target dependencies")
			}
			for _, id := range oldTarget.ProbeIDs {
				if !newKeys[key(oldByID[id])] {
					return fmt.Errorf("DNS transition drops a previous route dependency")
				}
			}
		}
		if !DNSReadinessQuorum(*record, func(DNSReadinessTarget) bool { return true }) {
			return fmt.Errorf("DNS transition cannot satisfy preserved quorum")
		}
	}
	slices.SortFunc(plan.Probes, func(a, b DNSReadinessProbe) int { return strings.Compare(a.ID, b.ID) })
	return ValidateDNSReadinessPlan(plan, policy.DNSReadiness)
}

// DNSRouteDependency is a content-derived publication precondition, used by the
// API and both durable stores. It does not authenticate or select a publication.
type DNSRouteDependency struct {
	GroupID         string
	Parent          model.PlatformArtifact
	ReleaseID       string
	ReleaseChannel  string
	CanaryRuleRef   string
	FencingToken    int64
	Artifacts       []model.PlatformArtifact
	RequireVerified bool
}

// DNSRouteDependencyEvidence shares one immutable JSON envelope across the
// typed dependency views. It is not signature or publication authorization.
type DNSRouteDependencyEvidence struct {
	Dependencies []DNSRouteDependency
	Publications []CellRoutePublicationInput
	Previous     *PreviousTrafficPublicationInput
}

func DNSRouteDependencies(a model.PlatformArtifact) ([]DNSRouteDependency, error) {
	evidence, err := DecodeDNSRouteDependencyEvidence(a)
	return evidence.Dependencies, err
}

func DecodeDNSRouteDependencyEvidence(a model.PlatformArtifact) (DNSRouteDependencyEvidence, error) {
	return DecodeDNSRouteDependencyEvidenceObserved(a, nil)
}

// DecodeDNSRouteDependencyEvidenceObserved preserves the exact decoder and
// error order while reporting fixed aggregate stages for bounded inventories.
// Timings are diagnostics, never part of a dependency or authorization proof.
func DecodeDNSRouteDependencyEvidenceObserved(a model.PlatformArtifact, cost *DNSDependencyDecodeCost) (evidence DNSRouteDependencyEvidence, err error) {
	if cost != nil {
		cost.Attempts++
		defer func() {
			if err != nil {
				cost.Rejected++
			}
		}()
	}
	started := cost.start()
	raw, err := json.Marshal(a.Content)
	cost.record(dnsDependencyEnvelopeEncode, started)
	if err != nil {
		return evidence, fmt.Errorf("DNS artifact content cannot be encoded: %w", err)
	}
	// Noncanonical aliases can merge typed fields while replacing RawMessages.
	// Keep their original decoding and error order, rather than changing that
	// compatibility behavior to enable reuse on the canonical compiler envelope.
	for key := range a.Content {
		for _, field := range []string{"policy", "cell_route_publications", "cell_dns_source", "previous_traffic_publication"} {
			if key != field && strings.EqualFold(key, field) {
				started = cost.start()
				evidence, err = decodeDNSRouteDependencyViews(raw)
				cost.record(dnsDependencyAliasFallback, started)
				return evidence, err
			}
		}
	}
	var payload struct {
		Publications dnsDependencyJSONView `json:"cell_route_publications"`
		Policy       dnsDependencyJSONView `json:"policy"`
		Source       *CellDNSPlanSource    `json:"cell_dns_source"`
		Previous     dnsDependencyJSONView `json:"previous_traffic_publication"`
	}
	started = cost.start()
	err = json.Unmarshal(raw, &payload)
	cost.record(dnsDependencyEnvelopeDecode, started)
	if err != nil {
		return evidence, fmt.Errorf("DNS artifact envelope cannot be decoded: %w", err)
	}
	return decodeDNSRouteDependencyFieldsObserved(payload.Publications, payload.Policy, payload.Previous, payload.Source, cost)
}

func decodeDNSRouteDependencyFieldsObserved(publications, policy, previousRaw []byte, source *CellDNSPlanSource, cost *DNSDependencyDecodeCost) (evidence DNSRouteDependencyEvidence, err error) {
	return decodeDNSRouteDependencyFieldsCached(publications, policy, previousRaw, source, cost, &dnsCellPublicationValidations)
}

func decodeDNSRouteDependencyFieldsCached(publications, policy, previousRaw []byte, source *CellDNSPlanSource, cost *DNSDependencyDecodeCost, cache *dnsValidationCache) (evidence DNSRouteDependencyEvidence, err error) {
	started := cost.start()
	pubs, _, err := decodeCellRoutePublicationFieldsCached(publications, policy, source, cost, cache)
	cost.record(dnsDependencyCells, started)
	if err != nil {
		return evidence, err
	}
	var previous *PreviousTrafficPublicationInput
	started = cost.start()
	var previousErr error
	if len(previousRaw) > 0 {
		previousErr = json.Unmarshal(previousRaw, &previous)
	}
	cost.record(dnsDependencyPreviousDecode, started)
	if previousErr != nil {
		return evidence, fmt.Errorf("previous traffic publication cannot be decoded")
	}
	started = cost.start()
	previous, err = validateDecodedPreviousTrafficPublication(previous, source)
	cost.record(dnsDependencyPreviousValidation, started)
	if err != nil {
		return evidence, err
	}
	started = cost.start()
	dependencies, err := dnsRouteDependenciesFromSource(source, pubs, previous)
	cost.record(dnsDependencySource, started)
	if err != nil {
		return evidence, err
	}
	evidence.Dependencies, evidence.Publications, evidence.Previous = dependencies, pubs, previous
	return evidence, nil
}

// The complete envelope is already owned by this decoder. Borrow its field
// bytes until the typed views have been decoded, rather than copying each large
// publication a second time as json.RawMessage does. These views never escape
// into evidence or a cache; all returned maps, strings and slices still come
// from the ordinary typed JSON decoders. Unmarshal validates the whole envelope
// and retains its original field/error order before any view is consumed.
type dnsDependencyJSONView []byte

func (v *dnsDependencyJSONView) UnmarshalJSON(raw []byte) error {
	*v = raw
	return nil
}

func decodeDNSRouteDependencyViews(raw []byte) (DNSRouteDependencyEvidence, error) {
	var evidence DNSRouteDependencyEvidence
	pubs, _, err := decodeCellRoutePublications(raw)
	if err != nil {
		return evidence, err
	}
	previous, err := decodePreviousTrafficPublication(raw)
	if err != nil {
		return evidence, err
	}
	dependencies, err := dnsRouteDependenciesFromDecoded(raw, pubs, previous)
	if err != nil {
		return evidence, err
	}
	evidence.Dependencies, evidence.Publications, evidence.Previous = dependencies, pubs, previous
	return evidence, nil
}

func dnsRouteDependenciesFromDecoded(raw []byte, pubs []CellRoutePublicationInput, previous *PreviousTrafficPublicationInput) ([]DNSRouteDependency, error) {
	if previous == nil {
		return dnsRouteDependenciesFromSource(nil, pubs, nil)
	}
	var payload struct {
		Source *CellDNSPlanSource `json:"cell_dns_source"`
	}
	if json.Unmarshal(raw, &payload) != nil {
		return nil, fmt.Errorf("DNS transition source missing")
	}
	return dnsRouteDependenciesFromSource(payload.Source, pubs, previous)
}

func dnsRouteDependenciesFromSource(source *CellDNSPlanSource, pubs []CellRoutePublicationInput, previous *PreviousTrafficPublicationInput) ([]DNSRouteDependency, error) {
	out := make([]DNSRouteDependency, 0, len(pubs)+2)
	for _, p := range pubs {
		r := p.Reference
		out = append(out, DNSRouteDependency{GroupID: r.AuthorityCellID, Parent: p.Parent, ReleaseID: r.ReleaseID, ReleaseChannel: r.ReleaseChannel, CanaryRuleRef: r.CanaryRuleRef, FencingToken: r.FencingToken, Artifacts: []model.PlatformArtifact{p.Parent, p.Route, p.TLS}})
	}
	if previous == nil {
		return out, nil
	}
	if source == nil {
		return nil, fmt.Errorf("DNS transition source missing")
	}
	aliases, err := RouteAuthorityAliases(source.Intent)
	if err != nil {
		return nil, err
	}
	groups := make([]string, 0, len(aliases))
	for group := range aliases {
		groups = append(groups, group)
	}
	slices.Sort(groups)
	for _, group := range groups {
		out = append(out, DNSRouteDependency{GroupID: group, Parent: previous.Parent, ReleaseID: previous.Reference.ReleaseID, ReleaseChannel: "full", FencingToken: previous.Reference.FencingToken, Artifacts: []model.PlatformArtifact{previous.Parent, previous.Route, previous.TLS, previous.DNS}, RequireVerified: true})
	}
	return out, nil
}
