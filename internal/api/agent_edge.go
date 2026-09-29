package api

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net/http"
	"net/netip"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"fugue/internal/agentedge"
	"fugue/internal/edgetopology"
	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/routebinding"
	"fugue/internal/routeprobe"
	"fugue/internal/routeproof"
	"fugue/internal/staticedgecontract"
)

var errAgentEdgeUnavailable = errors.New("authorized Agent Edge candidates unavailable")
var agentEdgeHintID = regexp.MustCompile(`^[a-z][a-z0-9-]{0,127}$`)

type agentEdgeProbeFunc func(context.Context, string, string, string, string, time.Duration) (routeprobe.Proof, error)

type agentEdgePreview struct {
	Ready             bool              `json:"ready"`
	AuthorizesTraffic bool              `json:"authorizes_traffic"`
	SigningKeyReady   bool              `json:"signing_key_ready"`
	Grant             *agentedge.Grant  `json:"grant,omitempty"`
	Diagnostics       map[string]string `json:"diagnostics"`
}

func (s *Server) handleGetAgentEdgePublicTrust(w http.ResponseWriter, r *http.Request) {
	p := mustPrincipal(r)
	if !p.IsPlatformAdmin() || !p.HasScope("artifact.read") {
		httpx.WriteError(w, 403, "platform admin artifact.read scope required")
		return
	}
	w.Header().Set("Cache-Control", "private, no-store")
	k, err := agentedge.LoadPrivateKeyring(s.agentEdgeSigningKeyFile)
	if err != nil {
		httpx.WriteError(w, 503, "Agent Edge trust configuration unavailable")
		return
	}
	httpx.WriteJSON(w, 200, k.Public())
}

func (s *Server) handlePreviewAgentEdgeCandidates(w http.ResponseWriter, r *http.Request) {
	p := mustPrincipal(r)
	if !p.IsPlatformAdmin() || !p.HasScope("artifact.read") {
		httpx.WriteError(w, 403, "platform admin artifact.read scope required")
		return
	}
	ids := r.URL.Query()["runtime_id"]
	if len(ids) != 1 || strings.TrimSpace(ids[0]) == "" || len(r.URL.Query()) != 1 {
		httpx.WriteError(w, 400, "exactly one runtime_id is required")
		return
	}
	w.Header().Set("Cache-Control", "private, no-store")
	grant, policy, diagnostics, err := s.captureAgentEdgeGrant(r.Context(), ids[0], "")
	result := agentEdgePreview{Diagnostics: diagnostics}
	if err == nil {
		result.Ready = true
		result.Grant = &grant
	}
	if k, e := agentedge.LoadPrivateKeyring(s.agentEdgeSigningKeyFile); e == nil {
		_, _, e = k.SigningKey(policy.SigningKeyID, time.Now())
		result.SigningKeyReady = e == nil
	}
	if result.Diagnostics == nil {
		result.Diagnostics = map[string]string{"authority": "unavailable"}
	}
	httpx.WriteJSON(w, 200, result)
}

func (s *Server) handleAgentEdgeCandidates(w http.ResponseWriter, r *http.Request) {
	p := mustPrincipal(r)
	if p.ActorType != model.ActorTypeRuntime {
		httpx.WriteError(w, 403, "runtime identity required")
		return
	}
	if len(r.URL.Query()) != 0 {
		httpx.WriteError(w, 400, "caller-selected grant identity is not supported")
		return
	}
	hint := r.Header.Get("X-Fugue-Agent-Current-Edge")
	if len(r.Header.Values("X-Fugue-Agent-Current-Edge")) > 1 || hint != "" && !agentEdgeHintID.MatchString(hint) {
		httpx.WriteError(w, 400, "invalid current Edge hint")
		return
	}
	w.Header().Set("Cache-Control", "private, no-store")
	grant, policy, _, err := s.captureAgentEdgeGrant(r.Context(), p.ActorID, hint)
	if err != nil {
		httpx.WriteError(w, 503, errAgentEdgeUnavailable.Error())
		return
	}
	ring, err := agentedge.LoadPrivateKeyring(s.agentEdgeSigningKeyFile)
	if err != nil {
		httpx.WriteError(w, 503, errAgentEdgeUnavailable.Error())
		return
	}
	private, key, err := ring.SigningKey(policy.SigningKeyID, time.Now())
	if err != nil {
		httpx.WriteError(w, 503, errAgentEdgeUnavailable.Error())
		return
	}
	if key.NotAfter.Before(grant.ValidUntil) {
		grant.ValidUntil = key.NotAfter
	}
	if grant.ValidUntil.Sub(time.Now()) < policy.MinimumLease() {
		httpx.WriteError(w, 503, errAgentEdgeUnavailable.Error())
		return
	}
	current, err := agentedge.LoadPrivateKeyring(s.agentEdgeSigningKeyFile)
	if err != nil || !reflect.DeepEqual(current, ring) || !s.agentGrantAuthorityUnchanged(grant) {
		httpx.WriteError(w, 503, errAgentEdgeUnavailable.Error())
		return
	}
	signed, err := agentedge.Sign(grant, policy.SigningKeyID, private)
	if err != nil {
		httpx.WriteError(w, 503, errAgentEdgeUnavailable.Error())
		return
	}
	httpx.WriteJSON(w, 200, signed)
}

type agentCellSource struct {
	cell         edgetopology.AuthorityCell
	parent       model.PlatformArtifact
	release      model.PlatformArtifactRelease
	route        model.PlatformArtifact
	publication  agentedge.Publication
	grant        edgetopology.RouteGrant
	paths        map[string]string
	probeTimeout time.Duration
}

func (s *Server) currentAgentAuthority() (agentedge.AuthorityPolicy, model.PlatformArtifact, model.PlatformArtifactRelease, error) {
	a, r, found, err := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindPolicySnapshot, agentedge.PolicyScope, "full")
	if err == nil && !found {
		a, r, found, err = s.store.GetActivePlatformArtifact(model.PlatformArtifactKindPolicySnapshot, agentedge.PolicyScope, "shadow")
	}
	if err != nil || !found || a.Status != model.PlatformArtifactStatusValidated || r.Status != model.PlatformArtifactReleaseStatusActive || r.ArtifactID != a.ID || s.store.VerifyPlatformArtifactIntegrity(a) != nil {
		return agentedge.AuthorityPolicy{}, a, r, errAgentEdgeUnavailable
	}
	p, err := agentedge.DecodeAuthorityPolicy(a)
	if err == nil && r.ReleaseChannel == "shadow" && p.Mode != "shadow" {
		err = errAgentEdgeUnavailable
	}
	return p, a, r, err
}

func (s *Server) agentTopology(p agentedge.AuthorityPolicy) (edgetopology.Intent, error) {
	a, err := s.store.GetPlatformArtifact(p.TopologyIntentArtifactID)
	if err != nil || a.ArtifactKind != model.PlatformArtifactKindPlatformIntent || a.ScopeKey != "global" || a.Status != model.PlatformArtifactStatusValidated || a.ContentHash != p.TopologyIntentDigest || s.store.VerifyPlatformArtifactIntegrity(a) != nil {
		return edgetopology.Intent{}, errAgentEdgeUnavailable
	}
	raw, _ := json.Marshal(a.Content)
	var intent platformconfig.PlatformIntent
	if staticedgecontract.StrictJSON(raw, &intent) != nil || platformconfig.ValidatePlatformIntent(intent) != nil || intent.EdgeTopology == nil || intent.EdgeTopology.Validate() != nil {
		return edgetopology.Intent{}, errAgentEdgeUnavailable
	}
	return intent.EdgeTopology.Clone(), nil
}

func (s *Server) agentCellAuthorization(p agentedge.AuthorityPolicy, topology edgetopology.Intent, cell edgetopology.AuthorityCell) (agentCellSource, error) {
	group := cell.ServingGroupID()
	snapshot, found, err := s.edgeRouteIntentSnapshotFromTrafficRelease(group)
	if err != nil || !found || snapshot.TrafficRelease == nil {
		return agentCellSource{}, errAgentEdgeUnavailable
	}
	b := snapshot.TrafficRelease
	parent, release, found, err := s.selectTrafficRouteRelease(group)
	if err != nil || !found || parent.ID != b.ReleaseSetID || parent.ContentHash != b.ReleaseSetDigest || release.ID != b.ReleaseID || release.FencingToken != b.FencingToken {
		return agentCellSource{}, errAgentEdgeUnavailable
	}
	route, err := s.store.GetPlatformArtifact(b.RouteArtifactID)
	if err != nil || route.ContentHash != b.RouteArtifactDigest || s.store.VerifyPlatformArtifactIntegrity(route) != nil {
		return agentCellSource{}, errAgentEdgeUnavailable
	}
	lineage := platformconfig.LineageFromArtifact(route)
	input, err := s.store.GetPlatformArtifactByIdentity(model.PlatformArtifactKindPlatformIntent, "global", lineage.IntentGeneration)
	if err != nil || input.Status != model.PlatformArtifactStatusValidated || s.store.VerifyPlatformArtifactIntegrity(input) != nil {
		return agentCellSource{}, errAgentEdgeUnavailable
	}
	var intent platformconfig.PlatformIntent
	raw, _ := json.Marshal(input.Content)
	if json.Unmarshal(raw, &intent) != nil || platformconfig.ValidatePlatformIntent(intent) != nil {
		return agentCellSource{}, errAgentEdgeUnavailable
	}
	digest, err := platformconfig.Digest(intent)
	if err != nil || digest != lineage.IntentDigest {
		return agentCellSource{}, errAgentEdgeUnavailable
	}
	var payload struct {
		Routes []platformconfig.CompiledRoute `json:"routes"`
		Policy platformconfig.PolicySnapshot  `json:"policy"`
	}
	raw, _ = json.Marshal(route.Content)
	if json.Unmarshal(raw, &payload) != nil {
		return agentCellSource{}, errAgentEdgeUnavailable
	}
	constraint, err := combineAgentConstraints(p.Constraint, payload.Policy.EdgeSelectionConstraints)
	if err != nil {
		return agentCellSource{}, err
	}
	intent.EdgeTopology = &topology
	payload.Policy.EdgeSelectionConstraints = []platformconfig.EdgeSelectionConstraint{constraint}
	grants, err := platformconfig.CompileAgentControlGrants(intent, payload.Policy, payload.Routes, snapshot)
	if err != nil || len(grants) != 1 || len(grants[0].RequiredRouteDigestsByCell[cell.ID]) == 0 {
		return agentCellSource{}, errAgentEdgeUnavailable
	}
	topologyDigest, _ := platformconfig.Digest(topology)
	result := agentCellSource{cell: cell, parent: parent, release: release, route: route, grant: grants[0], paths: map[string]string{}, publication: agentedge.Publication{
		ServingGroupID: group, ReleaseSetID: parent.ID, ReleaseSetDigest: parent.ContentHash, RouteArtifactID: route.ID, RouteArtifactDigest: route.ContentHash,
		PolicyDigest: b.PolicyDigest, IntentDigest: b.IntentDigest, InputSnapshotDigest: b.InputSnapshotDigest, TopologyDigest: topologyDigest, ScopeKey: "global",
		ReleaseID: release.ID, Channel: release.ReleaseChannel, FencingToken: release.FencingToken, PublishedAt: release.ReleasedAt}}
	result.probeTimeout = time.Duration(payload.Policy.DNSReadiness.ProbeTimeoutSeconds) * time.Second
	for _, r := range snapshot.Routes {
		if r.Hostname == constraint.Hostname {
			digest, err := routeproof.Digest(routebinding.FromIntent(r, group))
			if err != nil {
				return agentCellSource{}, err
			}
			path := r.PathPrefix
			if path == "" {
				path = "/"
			}
			result.paths[path] = digest
		}
	}
	// Local Agent latency measurements use the root route proof. Every other
	// path remains required in the server's signed evidence as well.
	if result.paths["/"] == "" || len(result.paths) > 64 {
		return agentCellSource{}, errAgentEdgeUnavailable
	}
	return result, nil
}

// A separately signed Agent policy can only narrow existing route grants.
func combineAgentConstraints(base platformconfig.EdgeSelectionConstraint, existing []platformconfig.EdgeSelectionConstraint) (platformconfig.EdgeSelectionConstraint, error) {
	base.MinDistinctDomains = cloneAgentMinimums(base.MinDistinctDomains)
	for _, c := range existing {
		if c.Hostname != base.Hostname {
			continue
		}
		if c.OwnerKind != "platform" || c.TenantID != "" {
			return base, errAgentEdgeUnavailable
		}
		intersect := func(a, b []string) []string {
			r := []string{}
			for _, v := range a {
				if slices.Contains(b, v) {
					r = append(r, v)
				}
			}
			return r
		}
		base.AllowedPoolIDs = intersect(base.AllowedPoolIDs, c.AllowedPoolIDs)
		if len(base.AllowedPoolIDs) == 0 {
			return base, errAgentEdgeUnavailable
		}
		if len(c.AllowedCountries) > 0 {
			if len(base.AllowedCountries) == 0 {
				base.AllowedCountries = slices.Clone(c.AllowedCountries)
			} else {
				base.AllowedCountries = intersect(base.AllowedCountries, c.AllowedCountries)
				if len(base.AllowedCountries) == 0 {
					return base, errAgentEdgeUnavailable
				}
			}
		}
		base.RequiredCapabilities = append(slices.Clone(base.RequiredCapabilities), c.RequiredCapabilities...)
		sort.Strings(base.RequiredCapabilities)
		base.RequiredCapabilities = slices.Compact(base.RequiredCapabilities)
		base.MinCandidates = max(base.MinCandidates, c.MinCandidates)
		base.MinDistinctCells = max(base.MinDistinctCells, c.MinDistinctCells)
		base.FactMaxAgeSeconds = min(base.FactMaxAgeSeconds, c.FactMaxAgeSeconds)
		for k, v := range c.MinDistinctDomains {
			base.MinDistinctDomains[k] = max(base.MinDistinctDomains[k], v)
		}
	}
	return base, platformconfig.ValidateEdgeSelectionConstraints([]platformconfig.EdgeSelectionConstraint{base})
}

func cloneAgentMinimums(m map[string]int) map[string]int {
	out := map[string]int{}
	for k, v := range m {
		out[k] = v
	}
	return out
}

func (s *Server) captureAgentEdgeGrant(ctx context.Context, audience, preferred string) (agentedge.Grant, agentedge.AuthorityPolicy, map[string]string, error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	diagnostics := map[string]string{}
	var result agentedge.Grant
	p, policyArtifact, policyRelease, err := s.currentAgentAuthority()
	if err != nil {
		diagnostics["authority"] = "policy_unavailable"
		return result, p, diagnostics, err
	}
	if _, err = s.store.GetRuntime(audience); err != nil {
		diagnostics["audience"] = "runtime_unavailable"
		return result, p, diagnostics, err
	}
	topology, err := s.agentTopology(p)
	if err != nil {
		diagnostics["authority"] = "topology_unavailable"
		return result, p, diagnostics, err
	}
	nodes, _, err := s.store.ListActiveEdgeNodes("")
	if err != nil {
		return result, p, diagnostics, err
	}
	byID := map[string]model.EdgeNode{}
	for _, node := range nodes {
		byID[node.ID] = node
	}
	quarantine := s.activeNodeQuarantineByName()
	factory := s.newClusterNodeClient
	if factory == nil {
		factory = newClusterNodeClient
	}
	client, err := factory()
	if err != nil {
		diagnostics["capacity"] = "authenticated_observer_unavailable"
		return result, p, diagnostics, err
	}
	defer client.closeIdleConnections()
	sources := map[string]agentCellSource{}
	for _, cell := range topology.Cells {
		if ctx.Err() != nil {
			return result, p, diagnostics, ctx.Err()
		}
		source, e := s.agentCellAuthorization(p, topology, cell)
		if e == nil {
			sources[cell.ID] = source
		}
	}
	type observation struct {
		candidate agentedge.Candidate
		fact      edgetopology.RouteFact
	}
	observations := []observation{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 4)
	prober := s.agentEdgeProbe
	if prober == nil {
		prober = routeprobe.Probe
	}
	type task struct {
		edge   edgetopology.Edge
		node   model.EdgeNode
		source agentCellSource
		rank   [32]byte
	}
	tasks := []task{}
	for _, edge := range topology.Edges {
		node, known := byID[edge.ID]
		source, allowed := sources[edge.AuthorityCellID]
		if !known || !allowed || node.EdgeGroupID != source.cell.ServingGroupID() || !edgeNodeRouteServingCapable(node, time.Now()) || edgeNodeQuarantined(node, quarantine) {
			diagnostics[edge.ID] = "inventory_or_authority_unavailable"
			continue
		}
		if !agentStaticEdgeAllowed(edge, source.grant) {
			diagnostics[edge.ID] = "policy_restricted"
			continue
		}
		tasks = append(tasks, task{edge: edge, node: node, source: source, rank: sha256.Sum256([]byte(audience + "\x00" + edge.ID))})
	}
	sort.Slice(tasks, func(i, j int) bool {
		if (tasks[i].edge.ID == preferred) != (tasks[j].edge.ID == preferred) {
			return tasks[i].edge.ID == preferred
		}
		return strings.Compare(string(tasks[i].rank[:]), string(tasks[j].rank[:])) < 0
	})
	// Preserve an allowed current choice and cover distinct cells before filling
	// the bounded observer window. A caller hint never changes authorization.
	chosen := []task{}
	used := map[string]bool{}
	cells := map[string]bool{}
	for _, t := range tasks {
		if t.edge.ID == preferred {
			chosen = append(chosen, t)
			used[t.edge.ID] = true
			cells[t.edge.AuthorityCellID] = true
			break
		}
	}
	for _, t := range tasks {
		if len(chosen) < p.Selection.MaxCandidates && len(cells) < p.Selection.DesiredDistinctCells && !cells[t.edge.AuthorityCellID] {
			chosen = append(chosen, t)
			used[t.edge.ID] = true
			cells[t.edge.AuthorityCellID] = true
		}
	}
	for _, t := range tasks {
		if !used[t.edge.ID] {
			if len(chosen) < p.Selection.MaxCandidates {
				chosen = append(chosen, t)
				used[t.edge.ID] = true
			} else {
				diagnostics[t.edge.ID] = "observation_budget"
			}
		}
	}
	for _, t := range chosen {
		wg.Add(1)
		go func(edge edgetopology.Edge, node model.EdgeNode, source agentCellSource) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				return
			}
			value, reason := captureAgentEdgeObservation(ctx, client, p, edge, node, source, prober)
			mu.Lock()
			defer mu.Unlock()
			if reason != "" {
				diagnostics[edge.ID] = reason
			} else {
				observations = append(observations, value)
			}
		}(t.edge, t.node, t.source)
	}
	wg.Wait()
	if ctx.Err() != nil {
		return result, p, diagnostics, ctx.Err()
	}
	latest, _, err := s.store.ListActiveEdgeNodes("")
	if err != nil {
		return result, p, diagnostics, err
	}
	live := map[string]model.EdgeNode{}
	for _, n := range latest {
		live[n.ID] = n
	}
	quarantine = s.activeNodeQuarantineByName()
	facts := []edgetopology.RouteFact{}
	eligibleObservations := []observation{}
	for _, o := range observations {
		n, ok := live[o.candidate.EdgeID]
		if !ok || !edgeNodeRouteServingCapable(n, time.Now()) || edgeNodeQuarantined(n, quarantine) || n.EdgeGroupID != o.fact.LegacyGroupID || !slices.Contains([]string{n.PublicIPv4, n.PublicIPv6}, o.candidate.Address) {
			diagnostics[o.candidate.EdgeID] = "inventory_changed"
			continue
		}
		facts = append(facts, o.fact)
		eligibleObservations = append(eligibleObservations, o)
	}
	result = agentedge.Grant{Schema: agentedge.GrantSchema, Purpose: agentedge.GrantPurpose, Audience: audience, Origin: p.Origin, Mode: p.Mode, Policy: p.Selection,
		PolicyReference:   agentedge.PolicyReference{ArtifactID: policyArtifact.ID, ArtifactDigest: policyArtifact.ContentHash, ReleaseID: policyRelease.ID, Channel: policyRelease.ReleaseChannel, FencingToken: policyRelease.FencingToken, PublishedAt: policyRelease.ReleasedAt},
		MinimumCandidates: p.Constraint.MinCandidates, MinDistinctCells: max(1, p.Constraint.MinDistinctCells), MinDistinctDomains: cloneAgentMinimums(p.Constraint.MinDistinctDomains), IssuedAt: time.Now().UTC()}
	result.ValidUntil = result.IssuedAt.Add(time.Duration(p.GrantTTLSeconds) * time.Second)
	for _, o := range eligibleObservations {
		source := sources[o.candidate.AuthorityCellID]
		eligible, e := topology.EligibleCandidates(source.grant, facts, result.IssuedAt)
		if e != nil || !slices.ContainsFunc(eligible, func(c edgetopology.Candidate) bool { return c.EdgeID == o.candidate.EdgeID }) {
			diagnostics[o.candidate.EdgeID] = "hard_constraint_unmet"
			continue
		}
		current, release, found, e := s.selectTrafficRouteRelease(source.cell.ServingGroupID())
		if e != nil || !found || current.ID != source.parent.ID || current.ContentHash != source.parent.ContentHash || release.ID != source.release.ID || release.FencingToken != source.release.FencingToken {
			// A cell's publication race removes that cell's observation only.
			// Other cells retain independent authority; final grant validation
			// still enforces every signed hard floor using surviving candidates.
			diagnostics[o.candidate.EdgeID] = "cell_publication_changed"
			continue
		}
		result.Candidates = append(result.Candidates, o.candidate)
		result.MinimumCandidates = max(result.MinimumCandidates, source.grant.MinCandidates)
		result.MinDistinctCells = max(result.MinDistinctCells, source.grant.MinDistinctCells)
		for k, v := range source.grant.MinDistinctDomains {
			result.MinDistinctDomains[k] = max(result.MinDistinctDomains[k], v)
		}
		if o.candidate.EvidenceValidUntil.Before(result.ValidUntil) {
			result.ValidUntil = o.candidate.EvidenceValidUntil
		}
	}
	sort.Slice(result.Candidates, func(i, j int) bool { return result.Candidates[i].EdgeID < result.Candidates[j].EdgeID })
	_, current, currentRelease, err := s.currentAgentAuthority()
	if err != nil || current.ID != policyArtifact.ID || current.ContentHash != policyArtifact.ContentHash || currentRelease.ID != policyRelease.ID || currentRelease.FencingToken != policyRelease.FencingToken ||
		result.ValidUntil.Sub(time.Now()) < p.MinimumLease() || result.Validate() != nil {
		return result, p, diagnostics, errAgentEdgeUnavailable
	}
	return result, p, diagnostics, nil
}

func captureAgentEdgeObservation(ctx context.Context, client *clusterNodeClient, p agentedge.AuthorityPolicy, edge edgetopology.Edge, node model.EdgeNode, source agentCellSource, probe agentEdgeProbeFunc) (struct {
	candidate agentedge.Candidate
	fact      edgetopology.RouteFact
}, string) {
	type resultType = struct {
		candidate agentedge.Candidate
		fact      edgetopology.RouteFact
	}
	fail := func(reason string) (resultType, string) { return resultType{}, reason }
	for _, address := range []string{node.PublicIPv4, node.PublicIPv6} {
		ip, err := netip.ParseAddr(address)
		if err != nil || !platformconfig.PublicDNSFlattenIP(ip) {
			continue
		}
		capacity, err := readAgentCapacity(ctx, client, node.ID, address, p.Capacity, time.Now)
		if err != nil {
			continue
		}
		observed, until := capacity.ObservedAt, capacity.ValidUntil
		if node.LastHeartbeatAt == nil || node.LastHeartbeatAt.After(time.Now()) {
			return fail("heartbeat_unavailable")
		}
		if node.LastHeartbeatAt.Before(observed) {
			observed = *node.LastHeartbeatAt
		}
		bound := node.LastHeartbeatAt.Add(time.Duration(source.grant.FactMaxAgeSeconds) * time.Second)
		if bound.Before(until) {
			until = bound
		}
		proofs := []routeprobe.Proof{}
		digests := []string{}
		valid := true
		paths := make([]string, 0, len(source.paths))
		for path := range source.paths {
			paths = append(paths, path)
		}
		sort.Strings(paths)
		for _, path := range paths {
			digest := source.paths[path]
			proof, err := probe(ctx, p.Constraint.Hostname, path, address, "", source.probeTimeout)
			b := proof.TrafficRelease
			if err != nil || proof.Version == "" || proof.Digest != digest || proof.EdgeID != edge.ID || proof.GroupID != source.cell.ServingGroupID() || proof.State != "" || proof.CheckedAt.IsZero() || proof.CheckedAt.After(time.Now()) || b == nil ||
				b.ReleaseSetID != source.parent.ID || b.ReleaseSetDigest != source.parent.ContentHash || b.RouteArtifactID != source.route.ID || b.RouteArtifactDigest != source.route.ContentHash ||
				b.ReleaseID != source.release.ID || b.ReleaseChannel != source.release.ReleaseChannel || b.FencingToken != source.release.FencingToken || b.ScopeKey != "global" ||
				b.PolicyDigest != source.publication.PolicyDigest || b.IntentDigest != source.publication.IntentDigest || b.InputSnapshotDigest != source.publication.InputSnapshotDigest {
				valid = false
				break
			}
			if proof.CheckedAt.Before(observed) {
				observed = proof.CheckedAt
			}
			if proof.ValidUntil.Before(until) {
				until = proof.ValidUntil
			}
			proofs = append(proofs, proof)
			digests = append(digests, digest)
		}
		bound = observed.Add(time.Duration(min(source.grant.FactMaxAgeSeconds, p.Selection.FactMaxAgeSeconds)) * time.Second)
		if bound.Before(until) {
			until = bound
		}
		if !valid || until.Sub(time.Now()) < p.MinimumLease() {
			continue
		}
		sort.Strings(digests)
		evidence, _ := platformconfig.Digest(map[string]any{"capacity": capacity, "routes": proofs, "inventory_heartbeat": node.LastHeartbeatAt})
		candidate := agentedge.Candidate{Publication: source.publication, EdgeID: edge.ID, AuthorityCellID: edge.AuthorityCellID, Address: address, RouteDigests: digests, FailureDomains: edge.FailureDomains, EvidenceDigest: evidence, EvidenceObservedAt: observed, EvidenceValidUntil: until}
		fact := edgetopology.RouteFact{EdgeID: edge.ID, LegacyGroupID: node.EdgeGroupID, ReadyRouteDigests: digests, TLSHostname: p.Constraint.Hostname, ObservedAt: observed, ValidUntil: until, Healthy: true, RouteReady: true, TLSReady: true, CapacityAvailable: true}
		return resultType{candidate: candidate, fact: fact}, ""
	}
	return fail("fresh_route_or_capacity_unavailable")
}

func agentStaticEdgeAllowed(edge edgetopology.Edge, g edgetopology.RouteGrant) bool {
	if slices.Contains(g.ExcludedEdgeIDs, edge.ID) || len(g.RequiredRouteDigestsByCell[edge.AuthorityCellID]) == 0 {
		return false
	}
	pool := false
	for _, id := range edge.ServingPoolIDs {
		pool = pool || slices.Contains(g.AllowedPoolIDs, id)
	}
	if !pool || len(g.AllowedCountries) > 0 && !slices.Contains(g.AllowedCountries, edge.Labels["country"]) {
		return false
	}
	for _, capability := range g.RequiredCapabilities {
		if !slices.Contains(edge.Capabilities, capability) {
			return false
		}
	}
	return true
}

func (s *Server) agentGrantAuthorityUnchanged(g agentedge.Grant) bool {
	_, a, r, err := s.currentAgentAuthority()
	if err != nil || a.ID != g.PolicyReference.ArtifactID || a.ContentHash != g.PolicyReference.ArtifactDigest || r.ID != g.PolicyReference.ReleaseID || r.FencingToken != g.PolicyReference.FencingToken {
		return false
	}
	seen := map[string]bool{}
	for _, c := range g.Candidates {
		if seen[c.AuthorityCellID] {
			continue
		}
		seen[c.AuthorityCellID] = true
		parent, release, found, err := s.selectTrafficRouteRelease(c.Publication.ServingGroupID)
		if err != nil || !found || parent.ID != c.Publication.ReleaseSetID || parent.ContentHash != c.Publication.ReleaseSetDigest || release.ID != c.Publication.ReleaseID || release.FencingToken != c.Publication.FencingToken {
			return false
		}
	}
	return true
}
