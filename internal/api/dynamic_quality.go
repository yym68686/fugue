package api

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"fugue/internal/dnsserver"
	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

type dynamicQualityJob struct {
	NodeID            string
	Route             platformconfig.PhysicalQualityRoute
	Key               string
	EvidenceHostname  string
	EvidenceHostnames []string
	EvidenceServices  []dynamicQualityService
}

type dynamicQualityService struct {
	Hostname, PathPrefix, TrafficClass string
}

func dynamicQualityServiceForRoute(route platformconfig.CompiledRoute) dynamicQualityService {
	service := dynamicQualityService{Hostname: route.Hostname, PathPrefix: route.PathPrefix, TrafficClass: "dynamic_api"}
	if service.PathPrefix == "" {
		service.PathPrefix = "/"
	}
	if route.Streaming != nil && *route.Streaming {
		service.TrafficClass = "streaming"
	}
	return service
}

func (service dynamicQualityService) key() string {
	return edgequality.ServiceEvidenceKey(edgequality.Snapshot{Hostname: service.Hostname, PathPrefix: service.PathPrefix, TrafficClass: service.TrafficClass})
}

type dynamicQualityEntry struct {
	Attempted     time.Time
	ContextDigest string
	Compiled      compiledPhysicalDNSSelection
	Failure       string
}

type dynamicQualityState struct {
	mu      sync.Mutex
	entries map[string]dynamicQualityEntry
}

func dynamicQualityRoutes(projection platformIntentProjectionResponse, policy platformconfig.DNSQueryPolicy) ([]dynamicQualityJob, map[string]string, error) {
	jobs, states := []dynamicQualityJob{}, map[string]string{}
	if policy.DynamicQuality == nil {
		return jobs, states, nil
	}
	explicit := map[string]bool{}
	for _, route := range policy.PhysicalRoutes {
		explicit[route.Hostname] = true
	}
	for _, record := range projection.Intent.DNS {
		if platformconfig.DNSPlacementOptions(record) == nil {
			states[record.Hostname] = "static_constraint"
			continue
		}
		if explicit[record.Hostname] {
			states[record.Hostname] = "explicit_quality_policy"
			continue
		}
		owners, _, err := placementRecordRoutes(projection, record)
		if err != nil {
			return nil, nil, err
		}
		pinned, streaming := false, true
		evidenceHostname := ""
		shared := false
		hostSet := map[string]bool{}
		serviceSet := map[string]dynamicQualityService{}
		for _, owner := range owners {
			pinned = pinned || owner.DNSPlacementEdgeGroupID != "" || owner.EdgeGroupMode == model.PlatformRouteEdgeGroupModePinned
			shared = shared || evidenceHostname != "" && evidenceHostname != owner.Hostname
			evidenceHostname = owner.Hostname
			hostSet[owner.Hostname] = true
			streaming = streaming && owner.Streaming != nil && *owner.Streaming
			service := dynamicQualityServiceForRoute(owner)
			serviceSet[service.key()] = service
		}
		if pinned {
			states[record.Hostname] = "pinned_constraint"
			continue
		}
		states[record.Hostname] = "learning_queued"
		if len(serviceSet) > 8 || evidenceHostname == "" {
			states[record.Hostname] = "learning_shared_owner_evidence_required"
			continue
		}
		evidenceHostnames := []string{}
		if shared {
			for hostname := range hostSet {
				evidenceHostnames = append(evidenceHostnames, hostname)
			}
			sort.Strings(evidenceHostnames)
			evidenceHostname = record.Hostname
		}
		class := "dynamic_api"
		if streaming {
			class = "streaming"
		}
		services := make([]dynamicQualityService, 0, len(serviceSet))
		for _, service := range serviceSet {
			services = append(services, service)
		}
		sort.Slice(services, func(left, right int) bool { return services[left].key() < services[right].key() })
		if len(services) > 0 {
			class = services[0].TrafficClass
		}
		for _, fact := range projection.RuntimeSnapshot.DNSSelections {
			if fact.Hostname != record.Hostname {
				continue
			}
			if fact.Type != "A" {
				states[record.Hostname] = "learning_address_family_evidence_required"
				continue
			}
			jobs = append(jobs, dynamicQualityJob{NodeID: fact.NodeID, Key: fact.NodeID + "\x00" + record.Hostname, EvidenceHostname: evidenceHostname, EvidenceHostnames: evidenceHostnames, EvidenceServices: services, Route: platformconfig.PhysicalQualityRoute{Hostname: record.Hostname, TrafficClass: class, Policy: policy.DynamicQuality.Policy}})
		}
	}
	sort.Slice(jobs, func(left, right int) bool { return jobs[left].Key < jobs[right].Key })
	return jobs, states, nil
}

func (state *dynamicQualityState) schedule(jobs []dynamicQualityJob, budget int, now time.Time) []dynamicQualityJob {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.entries == nil {
		state.entries = map[string]dynamicQualityEntry{}
	}
	active := map[string]bool{}
	for _, job := range jobs {
		active[job.Key] = true
	}
	for key := range state.entries {
		if !active[key] {
			delete(state.entries, key)
		}
	}
	ordered := append([]dynamicQualityJob(nil), jobs...)
	sort.SliceStable(ordered, func(left, right int) bool {
		return state.entries[ordered[left].Key].Attempted.Before(state.entries[ordered[right].Key].Attempted)
	})
	if len(ordered) > budget {
		ordered = ordered[:budget]
	}
	return ordered
}

func (state *dynamicQualityState) started(key string, now time.Time) {
	state.mu.Lock()
	defer state.mu.Unlock()
	entry := state.entries[key]
	entry.Attempted = now
	state.entries[key] = entry
}

func dynamicQualityContext(projection platformIntentProjectionResponse, job dynamicQualityJob) (string, error) {
	var record platformconfig.DNSIntent
	var owners []platformconfig.CompiledRoute
	for _, candidate := range projection.Intent.DNS {
		if candidate.Hostname != job.Route.Hostname || platformconfig.DNSPlacementOptions(candidate) == nil {
			continue
		}
		record = candidate
		var err error
		owners, _, err = placementRecordRoutes(projection, candidate)
		if err != nil {
			return "", err
		}
		break
	}
	var candidates []platformconfig.DNSSelectionCandidate
	for _, fact := range projection.RuntimeSnapshot.DNSSelections {
		if fact.NodeID == job.NodeID && fact.Hostname == job.Route.Hostname && fact.Type == "A" {
			candidates = fact.Candidates
		}
	}
	return platformconfig.Digest(struct {
		Record     platformconfig.DNSIntent
		Owners     []platformconfig.CompiledRoute
		Policy     model.PhysicalEdgeQualityPolicy
		Candidates []platformconfig.DNSSelectionCandidate
	}{record, owners, job.Route.Policy, candidates})
}

func (s *Server) captureDynamicQualityEvidence(ctx context.Context, job dynamicQualityJob, answer dnsserver.DNSDecisionReceipt) (edgequality.Receipt, error) {
	if len(job.EvidenceServices) == 0 {
		return s.capturePhysicalQualityForDNS(ctx, job.EvidenceHostname, job.Route.Hostname, job.Route.TrafficClass, edgeQualityRankScope{}, job.NodeID, job.Route.Policy, &answer)
	}
	if len(job.EvidenceServices) == 1 {
		service := job.EvidenceServices[0]
		return s.capturePhysicalQualityPathForDNS(ctx, service.Hostname, service.PathPrefix, job.Route.Hostname, service.TrafficClass, edgeQualityRankScope{}, job.NodeID, job.Route.Policy, &answer)
	}
	receipts := make([]edgequality.Receipt, len(job.EvidenceServices))
	errors := make([]error, len(receipts))
	var workers sync.WaitGroup
	for index, service := range job.EvidenceServices {
		workers.Add(1)
		go func(index int, service dynamicQualityService) {
			defer workers.Done()
			receipts[index], errors[index] = s.capturePhysicalQualityPathForDNS(ctx, service.Hostname, service.PathPrefix, job.Route.Hostname, service.TrafficClass, edgeQualityRankScope{}, job.NodeID, job.Route.Policy, &answer)
		}(index, service)
	}
	workers.Wait()
	for _, err := range errors {
		if err != nil {
			return edgequality.Receipt{}, err
		}
	}
	snapshot, err := edgequality.ServiceConsensusSnapshot(job.Route.Hostname, receipts)
	if err != nil {
		return edgequality.Receipt{}, err
	}
	return edgequality.Capture(snapshot)
}

func (s *Server) captureDynamicQuality(ctx context.Context, projection *platformIntentProjectionResponse, policy platformconfig.DNSQueryPolicy) error {
	if policy.DynamicQuality == nil {
		return nil
	}
	jobs, states, err := dynamicQualityRoutes(*projection, policy)
	if err != nil {
		return err
	}
	if len(jobs) > 20000 {
		return fmt.Errorf("dynamic quality query inventory exceeds bound")
	}
	if err := s.preservePublishedDynamicOrder(projection, jobs); err != nil {
		return err
	}
	work := s.dynamicQuality.schedule(jobs, policy.DynamicQuality.RefreshQueriesPerCycle, time.Now().UTC())
	captureBudget, cancelBudget := context.WithTimeout(ctx, 30*time.Second)
	defer cancelBudget()
	completed := captureDynamicQualityJobs(captureBudget, work, policy.DynamicQuality.RefreshConcurrency, func(captureContext context.Context, job dynamicQualityJob) {
		entry := dynamicQualityEntry{Attempted: time.Now().UTC()}
		s.dynamicQuality.started(job.Key, entry.Attempted)
		contextDigest, contextErr := dynamicQualityContext(*projection, job)
		if contextErr != nil {
			return
		}
		entry.ContextDigest = contextDigest
		answer, captureErr := s.observePhysicalDNSAnswer(captureContext, projection.RuntimeSnapshot.DNSConsumers, job.NodeID, job.Route.Hostname)
		if captureErr == nil {
			receipt, readErr := s.captureDynamicQualityEvidence(captureContext, job, answer)
			captureErr = readErr
			if captureErr == nil {
				entry.Compiled.Selection, captureErr = compileBoundPhysicalQualitySelection(receipt, time.Now().UTC())
				if captureErr == nil {
					entry.Compiled.Evidence, captureErr = json.Marshal(receipt)
				}
			}
		}
		if captureErr != nil {
			entry.Failure = captureErr.Error()
			entry.Compiled = compiledPhysicalDNSSelection{}
		}
		s.dynamicQuality.mu.Lock()
		s.dynamicQuality.entries[job.Key] = entry
		s.dynamicQuality.mu.Unlock()
	})
	if ctx.Err() != nil {
		return ctx.Err()
	}
	for _, job := range jobs {
		s.dynamicQuality.mu.Lock()
		entry := s.dynamicQuality.entries[job.Key]
		s.dynamicQuality.mu.Unlock()
		if entry.Failure != "" && s.log != nil {
			s.log.Printf("dynamic quality evidence unavailable; hostname=%s dns_node=%s error=%s", job.Route.Hostname, job.NodeID, entry.Failure)
		}
	}
	byHost := map[string][]dynamicQualityJob{}
	for _, job := range jobs {
		byHost[job.Route.Hostname] = append(byHost[job.Route.Hostname], job)
	}
	for hostname, hostJobs := range byHost {
		selections := map[string]compiledPhysicalDNSSelection{}
		for _, job := range hostJobs {
			digest, digestErr := dynamicQualityContext(*projection, job)
			s.dynamicQuality.mu.Lock()
			entry := s.dynamicQuality.entries[job.Key]
			s.dynamicQuality.mu.Unlock()
			if digestErr != nil || entry.ContextDigest != digest || entry.Compiled.Selection == nil || time.Since(entry.Compiled.Selection.CapturedAt) > time.Duration(job.Route.Policy.EvidenceMaxAgeSeconds)*time.Second {
				states[hostname] = "learning_evidence_unavailable"
				break
			}
			selections[job.Key] = entry.Compiled
		}
		if len(selections) != len(hostJobs) {
			continue
		}
		individual := policy
		individual.PhysicalRoutes = []platformconfig.PhysicalQualityRoute{hostJobs[0].Route}
		if err := applyPhysicalDNSSelections(projection, individual, selections, time.Now().UTC()); err != nil {
			states[hostname] = "learning_publication_validation_failed"
			if s.log != nil {
				s.log.Printf("dynamic quality publication validation failed; hostname=%s error=%v", hostname, err)
			}
			continue
		}
		states[hostname] = "measured"
		for _, compiled := range selections {
			if compiled.Selection.QualityState == "learning" {
				states[hostname] = "learning_current_ready_primary"
			}
		}
	}
	if projection.RuntimeSnapshot.Facts == nil {
		projection.RuntimeSnapshot.Facts = map[string]any{}
	}
	projection.RuntimeSnapshot.Facts["dynamic_quality_coverage"] = map[string]any{"mode": policy.DynamicQuality.Mode, "query_count": len(jobs), "scheduled_queries": len(work), "refreshed_queries": completed, "capture_budget_exhausted": captureBudget.Err() != nil, "hostnames": states}
	return nil
}

func captureDynamicQualityJobs(ctx context.Context, work []dynamicQualityJob, concurrency int, capture func(context.Context, dynamicQualityJob)) int {
	queue := make(chan dynamicQualityJob, len(work))
	for _, job := range work {
		queue <- job
	}
	close(queue)
	var workers sync.WaitGroup
	completed := make(chan struct{}, len(work))
	for index := 0; index < concurrency; index++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for job := range queue {
				if ctx.Err() != nil {
					return
				}
				captureContext, cancel := context.WithTimeout(ctx, 8*time.Second)
				capture(captureContext, job)
				cancel()
				completed <- struct{}{}
			}
		}()
	}
	workers.Wait()
	return len(completed)
}

func (s *Server) preservePublishedDynamicOrder(projection *platformIntentProjectionResponse, jobs []dynamicQualityJob) error {
	state, err := s.producedTrafficState(projection.Intent.Scope)
	if err != nil {
		return err
	}
	if state.lkg == nil || !state.hasFull || state.fullArtifact.ID != state.lkg.ArtifactID || !state.lkg.ExpiresAt.After(time.Now()) || s.store.VerifyPlatformArtifactIntegrity(state.fullArtifact) != nil {
		return fmt.Errorf("dynamic quality requires verified full serving baseline")
	}
	var set platformconfig.ReleaseSet
	if _, err := platformconfig.ValidateReleaseComposition(state.fullArtifact); err != nil {
		return err
	}
	raw, _ := json.Marshal(state.fullArtifact.Content)
	if json.Unmarshal(raw, &set) != nil {
		return fmt.Errorf("invalid dynamic quality baseline")
	}
	var member model.PlatformArtifact
	for index, kind := range set.ArtifactKinds {
		if kind == model.PlatformArtifactKindDNSAnswerBundle {
			member, err = s.store.GetPlatformArtifact(set.ArtifactIDs[index])
			if err != nil {
				return err
			}
		}
	}
	if member.ID == "" || s.store.VerifyPlatformArtifactIntegrity(member) != nil || member.Metadata["release_set_generation"] != state.fullArtifact.Generation {
		return fmt.Errorf("dynamic quality baseline DNS member invalid")
	}
	for _, key := range []string{"intent_digest", "policy_digest", "compiler_version", "input_snapshot_digest", "intent_generation", "policy_generation"} {
		if state.fullArtifact.Metadata[key] == "" || member.Metadata[key] != state.fullArtifact.Metadata[key] {
			return fmt.Errorf("dynamic quality baseline lineage mismatch")
		}
	}
	var payload struct {
		Views []platformconfig.DNSQueryView `json:"query_views"`
	}
	raw, _ = json.Marshal(member.Content)
	if json.Unmarshal(raw, &payload) != nil {
		return fmt.Errorf("dynamic quality baseline query views invalid")
	}
	s.restorePublishedQualityEvidence(*projection, jobs, state.fullArtifact)
	orders := map[string][]string{}
	epochs := map[string]*time.Time{}
	for _, view := range payload.Views {
		for _, record := range view.Records {
			key := view.NodeID + "\x00" + record.Name
			if record.Type != "A" {
				continue
			}
			if record.AnswerPolicy.PhysicalSelection != nil {
				orders[key] = record.AnswerPolicy.PhysicalSelection.OrderedEdgeIDs
				epochs[key] = record.AnswerPolicy.PhysicalSelection.PrimarySince
			} else if record.AnswerPolicy.PhysicalOrder != nil {
				orders[key] = record.AnswerPolicy.PhysicalOrder.OrderedEdgeIDs
				epochs[key] = record.AnswerPolicy.PhysicalOrder.PrimarySince
			}
		}
	}
	covered := map[string]bool{}
	for _, job := range jobs {
		covered[job.Key] = true
	}
	for index, rule := range projection.Policy.DNSAnswerRules {
		key := rule.NodeID + "\x00" + rule.Hostname
		if !covered[key] || rule.Type != "A" || rule.PhysicalOrder == nil {
			continue
		}
		if len(orders[key]) == 0 {
			continue
		}
		allowed := map[string]bool{}
		for _, edgeID := range rule.PhysicalOrder.OrderedEdgeIDs {
			allowed[edgeID] = true
		}
		order := []string{}
		for _, edgeID := range append(append([]string(nil), orders[key]...), rule.PhysicalOrder.OrderedEdgeIDs...) {
			if allowed[edgeID] {
				order = append(order, edgeID)
				delete(allowed, edgeID)
			}
		}
		projection.Policy.DNSAnswerRules[index].PhysicalOrder = &model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: order}
		if len(order) > 0 && order[0] == orders[key][0] && epochs[key] != nil {
			at := *epochs[key]
			projection.Policy.DNSAnswerRules[index].PhysicalOrder.PrimarySince = &at
		}
		for factIndex, fact := range projection.RuntimeSnapshot.DNSSelections {
			if fact.NodeID != rule.NodeID || fact.Hostname != rule.Hostname || fact.Type != rule.Type {
				continue
			}
			fact.Reason = "physical_quality_learning_preserved_verified_order"
			fact.SourceDigest, fact.SourceGeneration = "", ""
			digest, digestErr := platformconfig.Digest(fact)
			if digestErr != nil {
				return digestErr
			}
			fact.SourceDigest, fact.SourceGeneration = digest, "dns-observation_"+strings.TrimPrefix(digest, "sha256:")
			projection.RuntimeSnapshot.DNSSelections[factIndex] = fact
		}
	}
	generation, err := platformconfig.PolicySnapshotGeneration(projection.Policy)
	if err != nil {
		return err
	}
	projection.Policy.Generation, projection.RuntimeSnapshot.PolicyGeneration = generation, generation
	return nil
}

func (s *Server) restorePublishedQualityEvidence(projection platformIntentProjectionResponse, jobs []dynamicQualityJob, parent model.PlatformArtifact) {
	digest := parent.Metadata["input_snapshot_digest"]
	content, err := s.store.GetPlatformArtifactContent(digest)
	if err != nil {
		return
	}
	snapshot, err := platformconfig.DecodeRuntimeSnapshotContent(content.Content)
	actual, digestErr := platformconfig.RuntimeSnapshotDigest(snapshot)
	if err != nil || digestErr != nil || actual != digest || snapshot.IntentGeneration != parent.Metadata["intent_generation"] || snapshot.PolicyGeneration != parent.Metadata["policy_generation"] || snapshot.IntentGeneration != projection.Intent.Generation {
		return
	}
	requested := map[string]dynamicQualityJob{}
	for _, job := range jobs {
		requested[job.Key] = job
	}
	for _, fact := range snapshot.DNSSelections {
		job, found := requested[fact.NodeID+"\x00"+fact.Hostname]
		if !found || fact.PhysicalSelection == nil || len(fact.PhysicalEvidence) == 0 {
			continue
		}
		matchingCandidates := false
		for _, current := range projection.RuntimeSnapshot.DNSSelections {
			if current.NodeID == fact.NodeID && current.Hostname == fact.Hostname && current.Type == fact.Type {
				matchingCandidates = reflect.DeepEqual(current.Candidates, fact.Candidates)
				break
			}
		}
		if !matchingCandidates {
			continue
		}
		var receipt edgequality.Receipt
		if json.Unmarshal(fact.PhysicalEvidence, &receipt) != nil {
			continue
		}
		if receipt.Snapshot.Hostname != job.EvidenceHostname || edgequality.DNSHostname(receipt.Snapshot) != job.Route.Hostname {
			continue
		}
		expectedPolicy, _ := platformconfig.Digest(job.Route.Policy)
		capturedPolicy, _ := platformconfig.Digest(receipt.Snapshot.Policy)
		if expectedPolicy != capturedPolicy || time.Since(receipt.Snapshot.CapturedAt) > time.Duration(job.Route.Policy.EvidenceMaxAgeSeconds)*time.Second {
			continue
		}
		compiled, err := compileBoundPhysicalQualitySelection(receipt, receipt.Snapshot.CapturedAt)
		if err != nil {
			continue
		}
		declared, _ := platformconfig.Digest(fact.PhysicalSelection)
		derived, _ := platformconfig.Digest(compiled)
		if declared != derived {
			continue
		}
		contextDigest, err := dynamicQualityContext(projection, job)
		if err != nil {
			continue
		}
		s.dynamicQuality.mu.Lock()
		if s.dynamicQuality.entries == nil {
			s.dynamicQuality.entries = map[string]dynamicQualityEntry{}
		}
		entry := s.dynamicQuality.entries[job.Key]
		if entry.Compiled.Selection == nil || entry.Compiled.Selection.CapturedAt.Before(compiled.CapturedAt) {
			entry.ContextDigest = contextDigest
			entry.Compiled = compiledPhysicalDNSSelection{Selection: compiled, Evidence: append(json.RawMessage(nil), fact.PhysicalEvidence...)}
			s.dynamicQuality.entries[job.Key] = entry
		}
		s.dynamicQuality.mu.Unlock()
	}
}
