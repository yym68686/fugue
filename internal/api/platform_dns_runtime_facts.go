package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/cellpublication"
	"fugue/internal/dnsfacts"
	"fugue/internal/dnsroutesource"
	"fugue/internal/httpx"
	"fugue/internal/kubeauth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/routeartifact"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation"
)

var errDNSRuntimeFacts = errors.New("current DNS backend observations unavailable")

type dnsRuntimeBackend struct {
	Namespace   string `json:"namespace"`
	PodName     string `json:"pod_name"`
	PodUID      string `json:"pod_uid"`
	ServiceName string `json:"service_name"`
	ServiceUID  string `json:"service_uid"`
}

type platformDNSRuntimeFactsResponse struct {
	lkgGeneration       string
	heartbeatValidUntil time.Time
	Backend             dnsRuntimeBackend `json:"backend"`
	Snapshot            dnsfacts.Snapshot `json:"snapshot"`
	EvaluatedAt         time.Time         `json:"evaluated_at"`
	Ready               bool              `json:"ready"`
	ReadyProbeIDs       []string          `json:"ready_probe_ids"`
}

type dnsFactSource struct {
	cellBindings        map[string]*model.TrafficReleaseBinding
	heartbeatValidUntil time.Time
	consumer            model.PlatformConsumerInstance
	claims              platformcontrol.PlatformComponentIdentityClaims
	parent              model.PlatformArtifact
	lookup              consumerArtifactLookup
	payload             platformDNSArtifactPayload
	trafficBinding      *model.TrafficReleaseBinding
	group, routeID      string
}

func (s *Server) handleGetPlatformDNSRuntimeFacts(w http.ResponseWriter, r *http.Request) {
	p := mustPrincipal(r)
	if !p.IsPlatformAdmin() || !p.HasScope("artifact.read") {
		httpx.WriteError(w, http.StatusForbidden, "platform admin with artifact.read scope required")
		return
	}
	node := r.PathValue("node_id")
	if len(validation.IsDNS1123Subdomain(node)) != 0 {
		httpx.WriteError(w, http.StatusBadRequest, "canonical Kubernetes node identity required")
		return
	}
	w.Header().Set("Cache-Control", "private, no-store")
	result, err := s.readPlatformDNSRuntimeFacts(r.Context(), node)
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, errDNSRuntimeFacts.Error())
		return
	}
	httpx.WriteJSON(w, http.StatusOK, result)
}

func (s *Server) currentDNSFactSource(ctx context.Context, node string) (dnsFactSource, error) {
	return s.currentDNSFactSourceWithReaders(ctx, node, s.store.NewPlatformArtifactReadSessionContext(ctx), nil)
}

func (s *Server) currentDNSFactSourceWithReaders(ctx context.Context, node string, loadArtifact func(string) (model.PlatformArtifact, error), readBinding routeartifact.BindingReader) (dnsFactSource, error) {
	fail := func(reason string) (dnsFactSource, error) {
		if err := ctx.Err(); err != nil {
			observeCapture(ctx, captureEvent{Stage: "source_rejected", Node: node, Rejection: reason, ErrorClass: captureErrorClass(err)})
			return dnsFactSource{}, err
		}
		observeCapture(ctx, captureEvent{Stage: "source_rejected", Node: node, Rejection: reason})
		return dnsFactSource{}, errDNSRuntimeFacts
	}
	finish := startDNSReadStage(ctx, dnsReadConsumerInventory)
	consumers, err := s.store.ListPlatformAuthorityConsumersContext(ctx, model.PlatformArtifactKindDNSAnswerBundle, node)
	finish()
	if err != nil {
		return fail("consumer_inventory_read")
	}
	type candidate struct {
		fact   model.PlatformConsumerInstance
		claims platformcontrol.PlatformComponentIdentityClaims
	}
	var candidates []candidate
	seen := map[string]bool{}
	for i := range consumers {
		c := &consumers[i]
		if !c.IdentityVerified || c.NodeID != node || c.Component != model.PlatformConsumerComponentDNSServer || !strings.HasPrefix(c.CredentialID, "kubernetes:") {
			continue
		}
		finish = startDNSReadStage(ctx, dnsReadExpectedInventory)
		set, err := s.store.GetPlatformExpectedConsumerSetContext(ctx, c.ExpectedConsumerSetID)
		finish()
		if err != nil {
			return fail("expected_set_read")
		}
		var member *model.PlatformExpectedConsumer
		for _, expected := range platformcontrol.ProjectExpectedConsumerOwners(set).Consumers {
			if expected.ConsumerID == c.ConsumerID {
				if member != nil {
					return fail("ambiguous_expected_member")
				}
				copy := expected
				member = &copy
			}
		}
		if member == nil || member.HeartbeatFreshnessSeconds <= 0 {
			return fail("expected_member_missing")
		}
		claims := platformcontrol.PlatformComponentIdentityClaims{CredentialID: c.CredentialID, Component: c.Component, NodeID: node, ScopeKey: c.ScopeKey, ArtifactKinds: c.SupportedKinds, AuthorityID: member.AuthorityID}
		if !platformcontrol.ExpectedConsumerIdentityMatches(*member, claims) {
			return fail("consumer_identity")
		}
		if c.LastHeartbeatAt.IsZero() || !c.LastHeartbeatAt.Add(time.Duration(member.HeartbeatFreshnessSeconds)*time.Second).After(time.Now().UTC()) {
			observeCapture(ctx, captureEvent{Stage: "source_heartbeat_rejected", Node: node, Rejection: "consumer_heartbeat_stale"})
			continue
		}
		identityKey := c.ScopeKey + "/" + c.ConsumerID
		if seen[identityKey] {
			return fail("duplicate_consumer_receipt")
		}
		seen[identityKey] = true
		candidates = append(candidates, candidate{fact: *c, claims: claims})
	}
	var selected *dnsFactSource
	// Preserve a bounded sample of normally expected standby refusals only
	// if none of the candidates can provide current facts.
	var rejected [4]captureEvent
	rejectedCount := 0
	for _, candidate := range candidates {
		if len(candidates) > 1 {
			// Only transport can choose among retained authority receipts. Do
			// not prefer the most recent heartbeat or invent a physical node.
			finish = startDNSReadStage(ctx, dnsReadBackendDisambiguation)
			var reason string
			status := s.inspectDNSBackendTransport(ctx, candidate.claims, platformcontrol.PlatformConsumerHeartbeatEnvelope{}, nil, false, nil, &reason)
			finish()
			if status == http.StatusForbidden || status == http.StatusConflict {
				if rejectedCount < len(rejected) {
					rejected[rejectedCount] = captureEvent{Stage: "source_backend_rejected", Node: node, HTTPStatus: status, Rejection: reason}
				}
				rejectedCount++
				continue
			}
			if status != http.StatusOK {
				observeCapture(ctx, captureEvent{Stage: "source_backend_rejected", Node: node, HTTPStatus: status})
				return fail("backend_disambiguation")
			}
		}
		source, err := s.dnsFactSourceForConsumerWithReaders(ctx, node, candidate.fact, candidate.claims, loadArtifact, readBinding)
		if err != nil {
			return fail("consumer_assignment_unavailable")
		}
		if selected != nil {
			return fail("ambiguous_selected_source")
		}
		selected = &source
	}
	if selected == nil {
		for _, refusal := range rejected[:min(rejectedCount, len(rejected))] {
			observeCapture(ctx, refusal)
		}
		if rejectedCount > len(rejected) {
			observeCapture(ctx, captureEvent{Stage: "source_backend_rejections_truncated", Node: node})
		}
		return fail("no_current_source")
	}
	if err := ctx.Err(); err != nil {
		return dnsFactSource{}, err
	}
	return *selected, nil
}

func (s *Server) dnsFactSourceForConsumer(ctx context.Context, node string, fact model.PlatformConsumerInstance, claims platformcontrol.PlatformComponentIdentityClaims) (dnsFactSource, error) {
	return s.dnsFactSourceForConsumerWithReaders(ctx, node, fact, claims, s.store.NewPlatformArtifactReadSessionContext(ctx), nil)
}

func (s *Server) dnsFactSourceForConsumerWithReaders(ctx context.Context, node string, fact model.PlatformConsumerInstance, claims platformcontrol.PlatformComponentIdentityClaims, loadArtifact func(string) (model.PlatformArtifact, error), readBinding routeartifact.BindingReader) (dnsFactSource, error) {
	fail := func(reason string) (dnsFactSource, error) {
		observeCapture(ctx, captureEvent{Stage: "source_assignment_rejected", Node: node, Rejection: reason, ErrorClass: captureErrorClass(ctx.Err())})
		return dnsFactSource{}, errDNSRuntimeFacts
	}
	// Each authority observation owns a fresh envelope reader. Only the bounded
	// request-owned bodies and pure binding computation may cross observations.
	load := newConsumerArtifactReader(loadArtifact)
	readArtifact := func(id string) (model.PlatformArtifact, error) {
		if err := ctx.Err(); err != nil {
			return model.PlatformArtifact{}, err
		}
		return load(id)
	}
	finish := startDNSReadStage(ctx, dnsReadAssignmentResolution)
	resolved, err := s.resolvePlatformConsumerAssignmentsWithReaderContext(ctx, claims, readArtifact)
	finish()
	if err != nil {
		return fail("assignment_resolution")
	}
	var source *dnsFactSource
	for _, item := range resolved {
		if item.Artifact.ArtifactKind != model.PlatformArtifactKindDNSAnswerBundle || item.Release.ReleaseChannel == model.PlatformArtifactReleaseChannelShadow {
			continue
		}
		finish = startDNSReadStage(ctx, dnsReadAssignmentExpectedSet)
		set, err := s.store.GetPlatformExpectedConsumerSetContext(ctx, item.Assignment.ExpectedConsumerSetID)
		finish()
		if err != nil {
			return fail("assignment_expected_set_read")
		}
		group, freshness := "", 0
		for _, expected := range platformcontrol.ProjectExpectedConsumerOwners(set).Consumers {
			if platformcontrol.ExpectedConsumerIdentityMatches(expected, claims) {
				group, freshness = expected.Cohort, expected.HeartbeatFreshnessSeconds
			}
		}
		if group == "" || freshness <= 0 {
			return fail("assignment_expected_member_missing")
		}
		finish = startDNSReadStage(ctx, dnsReadTrafficReleaseSelection)
		parent, release, found, err := s.selectTrafficRouteReleaseInScopeContext(ctx, group, claims.ScopeKey)
		finish()
		if err != nil {
			return fail("traffic_release_selection")
		}
		if !found || release.ID != item.Release.ID {
			continue
		}
		now := time.Now().UTC()
		for _, check := range []struct {
			rejected bool
			reason   string
		}{
			{source != nil, "ambiguous_assignment"},
			{fact.ExpectedConsumerSetID != set.ID, "fact_expected_set"},
			{fact.ReleaseSetID != parent.ID, "fact_release_set"},
			{fact.FencingToken != release.FencingToken, "fact_fence"},
			{fact.GenerationSequence != item.Artifact.GenerationSequence, "fact_generation_sequence"},
			{fact.LastHeartbeatAt.IsZero(), "fact_heartbeat_missing"},
			{fact.LastHeartbeatAt.After(now), "fact_heartbeat_future"},
			{!fact.LastHeartbeatAt.Add(time.Duration(freshness) * time.Second).After(now), "fact_heartbeat_stale"},
		} {
			if check.rejected {
				observeCapture(ctx, captureEvent{Stage: "source_assignment_rejected", Node: node, Rejection: check.reason,
					DNSAuthority: captureDNSAuthorityEvidence(item.Assignment, fact)})
				return dnsFactSource{}, errDNSRuntimeFacts
			}
		}
		finish = startDNSReadStage(ctx, dnsReadArtifactDecode)
		payload, err := decodePlatformDNSArtifact(item.Artifact)
		finish()
		if err != nil || payload.ReadinessPlan == nil || payload.Policy.DNSReadiness == nil {
			return fail("artifact_payload")
		}
		viewFound := false
		for _, view := range payload.ConsumerViews {
			if view.NodeID == node {
				if view.EdgeGroupID != group {
					return fail("consumer_view_group")
				}
				viewFound = true
			}
		}
		if !viewFound {
			return fail("consumer_view_missing")
		}
		candidateSource := &dnsFactSource{consumer: fact, claims: claims, parent: parent, lookup: item, payload: payload, group: group, heartbeatValidUntil: fact.LastHeartbeatAt.Add(time.Duration(freshness) * time.Second)}
		if payload.Policy.PublicationRole == platformconfig.PublicationRoleCellDNS {
			finish = startDNSReadStage(ctx, dnsReadCellPublicationVerification)
			pubs, err := cellpublication.VerifyDNSArtifact(item.Artifact, s.bundleKeyring())
			finish()
			if err != nil {
				return fail("cell_publication_verification")
			}
			candidateSource.cellBindings = map[string]*model.TrafficReleaseBinding{}
			if payload.PreviousTrafficPublication != nil {
				if payload.CellDNSSource == nil {
					return fail("previous_traffic_source_missing")
				}
				finish = startDNSReadStage(ctx, dnsReadPreviousTrafficBinding)
				aliases, err := platformconfig.RouteAuthorityAliases(payload.CellDNSSource.Intent)
				if err != nil {
					finish()
					return fail("route_authority_aliases")
				}
				digest, _ := platformconfig.Digest(payload.PreviousTrafficPublication.Reference)
				for group := range aliases {
					binding, err := platformconfig.PreviousTrafficPublicationBinding(*payload.PreviousTrafficPublication, group)
					if err != nil {
						finish()
						return fail("previous_traffic_binding")
					}
					candidateSource.cellBindings[digest] = binding
					break
				}
				finish()
			}

			finish = startDNSReadStage(ctx, dnsReadCellRouteBindings)
			for _, input := range pubs {
				b, err := platformconfig.CellRoutePublicationBinding(input)
				if err != nil {
					finish()
					return fail("cell_route_binding")
				}
				digest, _ := platformconfig.Digest(input.Reference)
				candidateSource.cellBindings[digest] = b
			}
			finish()
		} else {
			finish = startDNSReadStage(ctx, dnsReadRouteProjection)
			projection, found, err := s.trafficRouteSourceWithInputCapture(ctx, group, claims.ScopeKey, readArtifact, readBinding, nil)
			finish()
			if err != nil || !found || projection.TrafficRelease == nil || projection.TrafficRelease.ReleaseSetID != parent.ID || projection.TrafficRelease.ReleaseID != release.ID || projection.TrafficRelease.FencingToken != release.FencingToken {
				return fail("route_projection")
			}
			candidateSource.routeID, candidateSource.trafficBinding = projection.TrafficRelease.RouteArtifactID, projection.TrafficRelease
		}
		source = candidateSource
	}
	if source == nil {
		return fail("selected_assignment_missing")
	}
	return *source, nil
}

func (s *Server) readPlatformDNSRuntimeFacts(ctx context.Context, node string) (response platformDNSRuntimeFactsResponse, readErr error) {
	ctx, trace := s.beginDNSRuntimeFactsCapture(ctx)
	stage := "source"
	var transportErr error
	var finishReplacementRetry, finishVerifiedReplacementRetry func()
	defer func() {
		if finishReplacementRetry != nil {
			finishReplacementRetry()
			if readErr == nil {
				finishVerifiedReplacementRetry()
			}
		}
		outcome := "ready"
		if readErr != nil {
			outcome = stage
		} else if !response.Ready {
			outcome = "not_ready"
		}
		observeCapture(ctx, captureEvent{Stage: stage, Node: node, ErrorClass: captureErrorClass(transportErr), HTTPStatus: captureHTTPStatus(transportErr), Rejection: captureDNSRejection(transportErr), DNSReplacement: captureDNSStateReplacement(transportErr)})
		s.finishCaptureTrace(trace, outcome, readErr, 0, 0)
	}()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	loadArtifact := s.store.NewPlatformArtifactReadSessionContext(ctx)
	readBinding := routeartifact.NewRequestBindingReader()
	phaseCtx, finishPhase := startDNSReadPhase(ctx, dnsReadInitialSource)
	source, err := s.currentDNSFactSourceWithReaders(phaseCtx, node, loadArtifact, readBinding)
	finishPhase()
	if err != nil {
		return platformDNSRuntimeFactsResponse{}, err
	}
	transportReady := false
	stage = "backend_selection"
	// Negative/expired observations are useful for diagnosis. Transport must be
	// uniquely selected and live, but a failing readiness probe must not hide it.
	phaseCtx, finishPhase = startDNSReadPhase(ctx, dnsReadBackend)
	status := s.inspectDNSBackend(phaseCtx, source.claims, platformcontrol.PlatformConsumerHeartbeatEnvelope{}, func(ctx context.Context, client *clusterNodeClient, pod corev1.Pod, svc corev1.Service, ready bool) int {
		transportReady = ready
		port, ok := dnsBackendObservationPort(pod, svc)
		if !ok {
			stage = "backend_observation_port"
			return http.StatusServiceUnavailable
		}
		stage = "backend_snapshot"
		path := "/api/v1/namespaces/" + url.PathEscape(pod.Namespace) + "/pods/" + url.PathEscape(pod.Name+":"+strconv.Itoa(int(port))) + "/proxy/runtime-facts"
		err := readDNSPodSnapshot(ctx, client, path, &response.Snapshot)
		// Retry only a discarded, concurrently replaced snapshot. Stay inside
		// the same transport observation (no new backend selection or deadline),
		// and still perform the post-read transport and authority checks.
		if captureHTTPStatus(err) == http.StatusServiceUnavailable && captureDNSRejection(err) == "state_replaced" && ctx.Err() == nil {
			observeCapture(ctx, captureEvent{Stage: "backend_snapshot_replacement_retry", Node: node,
				ErrorClass: captureErrorClass(err), HTTPStatus: http.StatusServiceUnavailable,
				Rejection: "state_replaced", DNSReplacement: captureDNSStateReplacement(err)})
			finishReplacementRetry = s.observeOperation("dns-snapshot-replacement-retry")
			finishVerifiedReplacementRetry = s.observeOperation("dns-snapshot-replacement-retry-verified")
			err = readDNSPodSnapshot(ctx, client, path, &response.Snapshot)
		}
		if err != nil {
			transportErr = err
			return http.StatusServiceUnavailable
		}
		response.Backend = dnsRuntimeBackend{Namespace: pod.Namespace, PodName: pod.Name, PodUID: string(pod.UID), ServiceName: svc.Name, ServiceUID: string(svc.UID)}
		return http.StatusOK
	})
	finishPhase()
	if status != http.StatusOK {
		if transportErr == nil {
			transportErr = &dnsObservationReadFailure{class: "backend_selection", status: status}
		}
		return platformDNSRuntimeFactsResponse{}, errDNSRuntimeFacts
	}
	// Re-resolve immutable inputs and current publication with fresh envelopes;
	// never retain an authority verdict across the network or renew heartbeat
	// time. The session rereads database metadata and actual content bytes; the
	// binding reader fingerprints complete inputs and current signing keys.
	stage = "authority_recheck"
	phaseCtx, finishPhase = startDNSReadPhase(ctx, dnsReadAuthorityRecheck)
	current, err := s.currentDNSFactSourceWithReaders(phaseCtx, node, loadArtifact, readBinding)
	finishPhase()
	if event := dnsAuthorityRecheckRejection(ctx, source, current, err); event.Rejection != "" {
		event.Node = node
		observeCapture(ctx, event)
		if err := ctx.Err(); err != nil {
			return platformDNSRuntimeFactsResponse{}, err
		}
		return platformDNSRuntimeFactsResponse{}, errDNSRuntimeFacts
	}
	_, finishPhase = startDNSReadPhase(ctx, dnsReadValidation)
	defer finishPhase()
	if response.Snapshot.RouteSources != nil {
		stage = "route_source_selection"
		observed, err := s.store.ObserveDNSRouteSources(ctx, current.lookup.Artifact)
		if err != nil || observed.SelectionDigest != response.Snapshot.RouteSources.Snapshot.SelectionDigest {
			return platformDNSRuntimeFactsResponse{}, errDNSRuntimeFacts
		}
		stage = "route_source_plan"
		plans, err := dnsroutesource.Build(current.lookup.Artifact, response.Snapshot.RouteSources.Snapshot, s.bundleKeyring(), time.Now().UTC())
		if err != nil {
			return platformDNSRuntimeFactsResponse{}, errDNSRuntimeFacts
		}
		if err := ctx.Err(); err != nil {
			return platformDNSRuntimeFactsResponse{}, err
		}
		stage = "route_source_replay"
		current.payload.ReadinessPlan, err = plans.Replay(response.Snapshot.RouteSources)
		if err != nil {
			return platformDNSRuntimeFactsResponse{}, errDNSRuntimeFacts
		}
		current.cellBindings = plans.Bindings
	} else {
		stage = "route_source_required"
		if approvals, err := platformconfig.DNSRouteSourceAuthorizations(current.lookup.Artifact); err != nil || len(approvals) > 0 {
			return platformDNSRuntimeFactsResponse{}, errDNSRuntimeFacts
		}
	}
	stage = "snapshot_validation"
	if err := ctx.Err(); err != nil {
		return platformDNSRuntimeFactsResponse{}, err
	}
	response.EvaluatedAt = time.Now().UTC()
	response.ReadyProbeIDs, response.Ready, err = evaluateDNSRuntimeSnapshotWithRejection(response.Snapshot, current, response.EvaluatedAt, func(reason string) {
		event := captureEvent{Stage: "snapshot_validation_rejected", Node: node, Rejection: reason}
		if reason == "assignment" {
			event.Mismatches = dnsAssignmentMismatchFields(response.Snapshot.Assignment, current.lookup.Assignment)
			event.DNSAuthority = captureDNSAuthorityEvidence(current.lookup.Assignment, current.consumer)
			snapshot := captureDNSAssignmentReference(response.Snapshot.Assignment)
			event.DNSAuthority.Snapshot = &snapshot
		}
		observeCapture(ctx, event)
	})
	response.Ready = response.Ready && transportReady
	response.lkgGeneration = current.consumer.LKGGeneration
	response.heartbeatValidUntil = current.heartbeatValidUntil
	if err := ctx.Err(); err != nil {
		return platformDNSRuntimeFactsResponse{}, err
	}
	if err != nil {
		return platformDNSRuntimeFactsResponse{}, errDNSRuntimeFacts
	}
	return response, nil
}

// Preserve the original short-circuit order, including evaluating ctx.Err only
// after every authority comparison passes. Retain the first failed predicate
// and field names, never a later read, credential, artifact body or error text.
func dnsAuthorityRecheckRejection(ctx context.Context, initial, current dnsFactSource, readErr error) captureEvent {
	event := captureEvent{Stage: "authority_recheck_rejected"}
	switch {
	case errors.Is(readErr, context.DeadlineExceeded), errors.Is(readErr, context.Canceled):
		event.Rejection, event.ErrorClass = "context", captureErrorClass(readErr)
	case readErr != nil:
		event.Rejection, event.ErrorClass = "source_unavailable", captureErrorClass(readErr)
	case current.consumer.CredentialID != initial.consumer.CredentialID:
		event.Rejection = "credential"
	case current.parent.ContentHash != initial.parent.ContentHash:
		event.Rejection = "parent_digest"
	case !reflect.DeepEqual(current.lookup.Assignment, initial.lookup.Assignment):
		event.Rejection = "assignment"
		event.Mismatches = dnsAssignmentMismatchFields(current.lookup.Assignment, initial.lookup.Assignment)
	case current.group != initial.group:
		event.Rejection = "group"
	default:
		if err := ctx.Err(); err != nil {
			event.Rejection, event.ErrorClass = "context", captureErrorClass(err)
		} else {
			return captureEvent{}
		}
	}
	return event
}

// Field names only: retain the exact failed comparison without exposing either
// assignment or accepting an old checkpoint. Keep DeepEqual semantics (including
// nil/empty capabilities and time metadata) identical to snapshot validation.
func dnsAssignmentMismatchFields(observed, expected model.PlatformConsumerAssignment) []string {
	fields := []string{}
	for _, check := range []struct {
		name string
		a, b any
	}{
		{"expected_consumer_set_id", observed.ExpectedConsumerSetID, expected.ExpectedConsumerSetID},
		{"release_set_id", observed.ReleaseSetID, expected.ReleaseSetID},
		{"artifact_release_id", observed.ArtifactReleaseID, expected.ArtifactReleaseID},
		{"artifact_kind", observed.ArtifactKind, expected.ArtifactKind},
		{"scope_key", observed.ScopeKey, expected.ScopeKey},
		{"expected_generation", observed.ExpectedGeneration, expected.ExpectedGeneration},
		{"revision", observed.Revision, expected.Revision},
		{"artifact_id", observed.ArtifactID, expected.ArtifactID},
		{"content_hash", observed.ContentHash, expected.ContentHash},
		{"generation_sequence", observed.GenerationSequence, expected.GenerationSequence},
		{"release_channel", observed.ReleaseChannel, expected.ReleaseChannel},
		{"fencing_token", observed.FencingToken, expected.FencingToken},
		{"expected_protocol_version", observed.ExpectedProtocolVersion, expected.ExpectedProtocolVersion},
		{"expected_schema_version", observed.ExpectedSchemaVersion, expected.ExpectedSchemaVersion},
		{"compatibility_capabilities", observed.CompatibilityCapabilities, expected.CompatibilityCapabilities},
		{"heartbeat_deadline", observed.HeartbeatDeadline, expected.HeartbeatDeadline},
		{"convergence_deadline", observed.ConvergenceDeadline, expected.ConvergenceDeadline},
	} {
		if !reflect.DeepEqual(check.a, check.b) {
			fields = append(fields, check.name)
		}
	}
	return fields
}

// Resolve the HTTP readiness port from the one container that owns all Service
// DNS target sockets. Names, container images and concrete port numbers are not
// inferred from projects or process environment.
func dnsBackendObservationPort(pod corev1.Pod, svc corev1.Service) (int32, bool) {
	var result int32
	matches := 0
	for _, container := range pod.Spec.Containers {
		owns := len(svc.Spec.Ports) > 0
		for _, required := range svc.Spec.Ports {
			count := 0
			for _, port := range container.Ports {
				if required.TargetPort.Type == intstr.Int && port.ContainerPort == required.TargetPort.IntVal && port.Protocol == required.Protocol {
					count++
				}
			}
			owns = owns && count == 1
		}
		if !owns {
			continue
		}
		matches++
		if container.ReadinessProbe == nil || container.ReadinessProbe.HTTPGet == nil {
			return 0, false
		}
		probe := container.ReadinessProbe.HTTPGet
		if probe.Host != "" || probe.Scheme != "" && probe.Scheme != corev1.URISchemeHTTP {
			return 0, false
		}
		if probe.Port.Type == intstr.Int {
			result = probe.Port.IntVal
		} else {
			count := 0
			for _, port := range container.Ports {
				if port.Name == probe.Port.StrVal && port.Protocol == corev1.ProtocolTCP {
					result = port.ContainerPort
					count++
				}
			}
			if count != 1 {
				return 0, false
			}
		}
	}
	return result, matches == 1 && result > 0 && result <= 65535
}

func readDNSPodSnapshot(ctx context.Context, client *clusterNodeClient, path string, out *dnsfacts.Snapshot) error {
	ctx = kubeauth.WithObservationSite(ctx, kubeauth.ObserveDNSRuntimeFacts)
	return readDNSPodObservation(ctx, client, path, out, 32<<20)
}

func readDNSPodObservation(ctx context.Context, client *clusterNodeClient, path string, out any, maximum int64) error {
	return readPodObservation(ctx, client, path, out, maximum, nil)
}

func readPodObservation(ctx context.Context, client *clusterNodeClient, path string, out any, maximum int64, classify func(*http.Response) error) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, client.baseURL+path, nil)
	if err != nil {
		return &dnsObservationReadFailure{class: "request"}
	}
	req.Header.Set("Authorization", "Bearer "+client.bearerToken)
	req.Header.Set("Accept", "application/json")
	reader := *client.client
	reader.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := reader.Do(req)
	if err != nil {
		return &dnsObservationReadFailure{class: captureErrorClass(err)}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		if classify != nil {
			if err := classify(resp); err != nil {
				return err
			}
		}
		var rejection string
		switch values := resp.Header.Values("X-Fugue-DNS-Observation-Rejection"); len(values) {
		case 0:
		case 1:
			rejection = dnsObservationRejection(values[0])
		default:
			rejection = "unrecognized"
		}
		return &dnsObservationReadFailure{class: "http_status", status: resp.StatusCode, rejection: rejection,
			replacement: dnsObservationStateReplacement(resp.StatusCode, rejection, resp.Header.Values)}
	}
	if failure := decodePodObservationBody(resp.Body, out, maximum); failure != "" {
		return &dnsObservationReadFailure{class: failure, status: resp.StatusCode}
	}
	return nil
}

type dnsObservationBodyReader struct {
	io.Reader
	err error
}

func (r *dnsObservationBodyReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if err != nil && err != io.EOF {
		r.err = err
	}
	return n, err
}

// Decode from the bounded body without first making a full copy of the same
// JSON in io.ReadAll. Unknown fields and trailing values still fail closed.
// Drain only within the original budget on a decode error to distinguish
// truncated or oversized responses from malformed JSON as before.
func decodePodObservationBody(body io.Reader, out any, maximum int64) string {
	reader := &dnsObservationBodyReader{Reader: body}
	limited := &io.LimitedReader{R: reader, N: maximum + 1}
	decoder := json.NewDecoder(limited)
	decoder.DisallowUnknownFields()
	valid := decoder.Decode(out) == nil && decoder.Decode(&struct{}{}) == io.EOF
	if !valid && reader.err == nil {
		_, _ = io.Copy(io.Discard, limited)
	}
	switch {
	case reader.err != nil:
		return captureErrorClass(reader.err)
	case limited.N == 0:
		return "response_budget"
	case !valid:
		return "decode"
	default:
		return ""
	}
}

func evaluateDNSRuntimeSnapshot(snapshot dnsfacts.Snapshot, source dnsFactSource, now time.Time) ([]string, bool, error) {
	return evaluateDNSRuntimeSnapshotWithRejection(snapshot, source, now, nil)
}

// Rejection names are fixed local metadata. They neither replace validation nor
// expose the snapshot, proof, hostnames, credentials or raw error text.
func evaluateDNSRuntimeSnapshotWithRejection(snapshot dnsfacts.Snapshot, source dnsFactSource, now time.Time, reject func(string)) ([]string, bool, error) {
	fail := func(reason string) ([]string, bool, error) {
		if reject != nil {
			reject(reason)
		}
		return nil, false, errDNSRuntimeFacts
	}
	plan, policy := source.payload.ReadinessPlan, source.payload.Policy.DNSReadiness
	if plan == nil || policy == nil {
		return fail("readiness_contract_missing")
	}
	digest, err := platformconfig.Digest(plan)
	if err != nil {
		return fail("plan_digest_unavailable")
	}
	for _, check := range []struct {
		rejected bool
		reason   string
	}{
		{snapshot.Schema != dnsfacts.Schema, "schema"},
		{snapshot.NodeID != source.claims.NodeID, "node_identity"},
		{snapshot.EdgeGroupID != source.group, "group_identity"},
		{snapshot.ParentDigest != source.parent.ContentHash, "parent_digest"},
		{snapshot.RouteArtifactID != source.routeID, "route_artifact"},
		{snapshot.PlanDigest != digest, "plan_digest"},
		{!reflect.DeepEqual(snapshot.Assignment, source.lookup.Assignment), "assignment"},
		{snapshot.ObservedAt.IsZero(), "observed_at_missing"},
		{snapshot.EvaluatedAt.Before(snapshot.ObservedAt), "evaluated_before_observation"},
		{snapshot.EvaluatedAt.After(now), "evaluated_in_future"},
		{snapshot.CheckpointValidUntil.IsZero(), "checkpoint_deadline_missing"},
		{snapshot.CheckpointValidUntil.After(snapshot.EvaluatedAt.Add(time.Duration(source.payload.Policy.MaxStaleSeconds) * time.Second)), "checkpoint_deadline_exceeds_policy"},
		{len(snapshot.Facts) > 4096, "fact_count_budget"},
	} {
		if check.rejected {
			return fail(check.reason)
		}
	}
	requirements := map[string]platformconfig.DNSReadinessProbe{}
	for _, p := range plan.Probes {
		requirements[p.ID] = p
	}
	seen, valid := map[string]bool{}, map[string]bool{}
	factsByID := map[string]dnsfacts.Probe{}
	for _, fact := range snapshot.Facts {
		req, found := requirements[fact.ProbeID]
		if !found {
			return fail("probe_not_in_plan")
		}
		if seen[fact.ProbeID] {
			return fail("duplicate_probe")
		}
		seen[fact.ProbeID] = true
		factsByID[fact.ProbeID] = fact
		if !fact.Ready {
			continue
		}
		p, b := fact.Proof, fact.Proof.TrafficRelease
		if until := snapshot.RouteSources.ProofDeadline(b); !until.IsZero() && p.ValidUntil.After(until) {
			return fail("proof_exceeds_source_deadline")
		}
		bindingMatches := source.trafficBinding != nil && reflect.DeepEqual(b, source.trafficBinding)
		if source.payload.Policy.PublicationRole == platformconfig.PublicationRoleCellDNS {
			bindingMatches = false
			for _, ref := range platformconfig.DNSReadinessProofReferences(req, p.EdgeID, p.GroupID, p.Digest) {
				expected := source.cellBindings[ref]
				bindingMatches = bindingMatches || expected != nil && reflect.DeepEqual(b, expected)
			}
		}
		for _, check := range []struct {
			rejected bool
			reason   string
		}{
			{!platformconfig.DNSReadinessProofMatches(req, p.EdgeID, p.GroupID, p.Digest), "proof_requirement"},
			{p.State != req.State, "proof_state"},
			{p.Version == "", "proof_version_missing"},
			{p.CheckedAt.IsZero(), "proof_observed_at_missing"},
			{p.CheckedAt.After(snapshot.EvaluatedAt), "proof_observed_after_evaluation"},
			{p.ValidUntil.After(p.CheckedAt.Add(time.Duration(platformconfig.DNSReadinessFactMaxAge(req, policy)) * time.Second)), "proof_deadline_exceeds_policy"},
			{b == nil, "proof_publication_missing"},
			{!bindingMatches, "proof_publication_binding"},
		} {
			if check.rejected {
				return fail(check.reason)
			}
		}
		valid[fact.ProbeID] = p.ValidUntil.After(now) && snapshot.CheckpointValidUntil.After(now)
	}
	ready := snapshot.Ready && snapshot.CheckpointValidUntil.After(now)
	for _, record := range plan.Records {
		ready = ready && platformconfig.DNSReadinessQuorum(record, func(target platformconfig.DNSReadinessTarget) bool {
			return platformconfig.DNSReadinessTargetReady(target, func(id string) (string, *model.TrafficReleaseBinding, bool) {
				fact := factsByID[id]
				return fact.Proof.GroupID, fact.Proof.TrafficRelease, valid[id]
			})
		})
	}
	ids := []string{}
	for id, pass := range valid {
		if pass {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids, ready, nil
}
