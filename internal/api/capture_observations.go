package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"fugue/internal/dnsfacts"
	"fugue/internal/model"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// These receipts are local-only, bounded, metadata-only evidence. They do not
// authorize a candidate, renew a fact or store tenant identity, paths or errors.
const captureReceiptLimit = 24
const captureEventLimit = 12
const dnsCaptureFailureLimit = 8

type captureObservations struct {
	mu                     sync.Mutex
	sequence               uint64
	completed              uint64
	evicted                uint64
	receipts               []captureReceipt
	agentFailures          []captureReceipt
	agentFailureTotal      uint64
	agentFailureEvicted    uint64
	dnsFailures            []captureReceipt
	dnsFailureTotal        uint64
	dnsFailureEvicted      uint64
	trafficFailures        []trafficDiagnosticFailure
	trafficFailureTotal    uint64
	trafficFailureEvicted  uint64
	consumerFailures       []consumerRequestFailure
	consumerFailureTotal   uint64
	consumerFailureEvicted uint64
}

type captureReceipt struct {
	Sequence            uint64                    `json:"sequence"`
	DatabaseOperationID uint64                    `json:"database_operation_id,omitempty"`
	Kind                string                    `json:"kind"`
	StartedAt           time.Time                 `json:"started_at"`
	FinishedAt          time.Time                 `json:"finished_at"`
	Outcome             string                    `json:"outcome"`
	ErrorClass          string                    `json:"error_class,omitempty"`
	Candidates          int                       `json:"candidates"`
	Minimum             int                       `json:"minimum_candidates"`
	Events              []captureEvent            `json:"events"`
	Dropped             uint64                    `json:"events_dropped"`
	DNSRead             []dnsReadStageObservation `json:"dns_read,omitempty"`
}

type captureEvent struct {
	Stage          string                  `json:"stage"`
	Node           string                  `json:"node,omitempty"`
	ErrorClass     string                  `json:"error_class,omitempty"`
	DurationMS     int64                   `json:"duration_ms"`
	Capacity       *captureCapacity        `json:"capacity,omitempty"`
	Mismatches     []string                `json:"mismatches,omitempty"`
	HTTPStatus     int                     `json:"http_status,omitempty"`
	Rejection      string                  `json:"rejection,omitempty"`
	Publication    *capturePublicationPair `json:"publication,omitempty"`
	DNSAuthority   *captureDNSAuthority    `json:"dns_authority,omitempty"`
	DNSParent      *captureDNSParent       `json:"dns_parent,omitempty"`
	DNSReplacement *captureDNSReplacement  `json:"dns_replacement,omitempty"`
}

// Observe only the two inputs to the rejected comparison. A backend UID is
// available for a snapshot comparison, but not an authority-to-authority read.
// Hash even the claimed digest: on the snapshot side it is a wire value.
type captureDNSParent struct {
	ExpectedDigestHash string `json:"expected_digest_hash"`
	ObservedDigestHash string `json:"observed_digest_hash"`
	BackendPodUIDHash  string `json:"backend_pod_uid_hash,omitempty"`
}

func captureDNSParentEvidence(expected, observed, backendUID string) *captureDNSParent {
	return &captureDNSParent{ExpectedDigestHash: captureIdentityHash(expected),
		ObservedDigestHash: captureIdentityHash(observed), BackendPodUIDHash: captureIdentityHash(backendUID)}
}

type captureDNSReplacement struct {
	Base      string `json:"base"`
	Selection string `json:"selection"`
	Renewal   string `json:"renewal"`
}

// These values come from the exact failed comparison. No subsequent store
// read may replace them with a newer heartbeat or expectation. All opaque
// references are hashed; credentials, nonces, evidence and bodies are omitted.
type captureDNSAssignment struct {
	ExpectedSetHash     string    `json:"expected_set_hash"`
	ReleaseSetHash      string    `json:"release_set_hash"`
	ReleaseHash         string    `json:"release_hash"`
	ArtifactHash        string    `json:"artifact_hash"`
	GenerationSequence  int64     `json:"generation_sequence"`
	Revision            int64     `json:"revision"`
	FencingToken        int64     `json:"fencing_token"`
	Channel             string    `json:"channel"`
	HeartbeatDeadline   time.Time `json:"heartbeat_deadline"`
	ConvergenceDeadline time.Time `json:"convergence_deadline"`
}

type captureDNSHeartbeat struct {
	ExpectedSetHash    string    `json:"expected_set_hash"`
	ReleaseSetHash     string    `json:"release_set_hash"`
	GenerationSequence int64     `json:"generation_sequence"`
	FencingToken       int64     `json:"fencing_token"`
	LastHeartbeatAt    time.Time `json:"last_heartbeat_at"`
}

type captureDNSAuthority struct {
	Expected  captureDNSAssignment  `json:"expected"`
	Heartbeat captureDNSHeartbeat   `json:"heartbeat"`
	Snapshot  *captureDNSAssignment `json:"snapshot,omitempty"`
}

func captureDNSAssignmentReference(a model.PlatformConsumerAssignment) captureDNSAssignment {
	publication := capturePublicationReference(a.ReleaseSetID, a.ArtifactID, a.ArtifactReleaseID, a.ReleaseChannel, a.FencingToken)
	return captureDNSAssignment{ExpectedSetHash: captureIdentityHash(a.ExpectedConsumerSetID),
		ReleaseSetHash: publication.ReleaseSetHash, ReleaseHash: publication.ReleaseHash, ArtifactHash: publication.RouteArtifactHash,
		GenerationSequence: a.GenerationSequence, Revision: a.Revision, FencingToken: a.FencingToken, Channel: publication.Channel,
		HeartbeatDeadline: a.HeartbeatDeadline, ConvergenceDeadline: a.ConvergenceDeadline}
}

func captureDNSAuthorityEvidence(a model.PlatformConsumerAssignment, fact model.PlatformConsumerInstance) *captureDNSAuthority {
	return &captureDNSAuthority{Expected: captureDNSAssignmentReference(a), Heartbeat: captureDNSHeartbeat{
		ExpectedSetHash: captureIdentityHash(fact.ExpectedConsumerSetID), ReleaseSetHash: captureIdentityHash(fact.ReleaseSetID),
		GenerationSequence: fact.GenerationSequence, FencingToken: fact.FencingToken, LastHeartbeatAt: fact.LastHeartbeatAt}}
}

// Publication references are hashed independently so a local read-only
// inventory can identify both sides of one rejected proof without retaining
// the proof, raw identities, Host/path, credentials or arbitrary wire strings.
type capturePublication struct {
	ReleaseSetHash    string `json:"release_set_hash"`
	RouteArtifactHash string `json:"route_artifact_hash"`
	ReleaseHash       string `json:"release_hash"`
	Channel           string `json:"channel"`
	FencingToken      int64  `json:"fencing_token"`
}

type capturePublicationPair struct {
	Expected capturePublication  `json:"expected"`
	Observed *capturePublication `json:"observed,omitempty"`
}

func captureIdentityHash(value string) string {
	if value == "" {
		return ""
	}
	if len(value) > 256 {
		return "identity_exceeds_budget"
	}
	sum := sha256.Sum256([]byte(value))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func capturePublicationReference(releaseSet, routeArtifact, release, channel string, fence int64) capturePublication {
	switch channel {
	case model.PlatformArtifactReleaseChannelShadow, model.PlatformArtifactReleaseChannelGray, model.PlatformArtifactReleaseChannelFull:
	default:
		channel = "unrecognized"
	}
	return capturePublication{ReleaseSetHash: captureIdentityHash(releaseSet), RouteArtifactHash: captureIdentityHash(routeArtifact),
		ReleaseHash: captureIdentityHash(release), Channel: channel, FencingToken: fence}
}

func agentPublicationEvidence(source agentCellSource, proof *model.TrafficReleaseBinding, proofAvailable bool) *capturePublicationPair {
	pair := &capturePublicationPair{Expected: capturePublicationReference(source.parent.ID, source.route.ID, source.release.ID, source.release.ReleaseChannel, source.release.FencingToken)}
	// A transport/parser failure must never label a partial proof as observed.
	if proofAvailable && proof != nil {
		observed := capturePublicationReference(proof.ReleaseSetID, proof.RouteArtifactID, proof.ReleaseID, proof.ReleaseChannel, proof.FencingToken)
		pair.Observed = &observed
	}
	return pair
}

type captureCapacity struct {
	NodeUID            string    `json:"node_uid"`
	Unschedulable      bool      `json:"unschedulable"`
	AddressOwned       bool      `json:"address_owned"`
	NodeReady          string    `json:"node_ready,omitempty"`
	MemoryPressure     string    `json:"memory_pressure,omitempty"`
	DiskPressure       string    `json:"disk_pressure,omitempty"`
	PIDPressure        string    `json:"pid_pressure,omitempty"`
	NetworkUnavailable string    `json:"network_unavailable,omitempty"`
	CPUTime            time.Time `json:"cpu_time"`
	MemoryTime         time.Time `json:"memory_time"`
	CPUNanoCores       uint64    `json:"cpu_nanocores"`
	CPUMilliCores      int64     `json:"cpu_allocatable_millicores"`
	MemoryBytes        uint64    `json:"memory_working_set_bytes"`
	MemoryAvailable    int64     `json:"memory_allocatable_bytes"`
	MaxCPUPercent      int       `json:"max_cpu_percent"`
	MaxMemoryPercent   int       `json:"max_memory_percent"`
	ValidUntil         time.Time `json:"valid_until"`
}

type captureTrace struct {
	mu            sync.Mutex
	receipt       captureReceipt
	dnsRead       *dnsReadTimings
	dnsReadClosed bool
}

type captureTraceKey struct{}
type capturePurposeKey struct{}

func (s *Server) beginCaptureTrace(ctx context.Context, kind string) (context.Context, *captureTrace) {
	s.captures.mu.Lock()
	s.captures.sequence++
	sequence := s.captures.sequence
	s.captures.mu.Unlock()
	t := &captureTrace{receipt: captureReceipt{Sequence: sequence, Kind: kind, StartedAt: time.Now().UTC()}}
	return context.WithValue(ctx, captureTraceKey{}, t), t
}

func observeCapture(ctx context.Context, event captureEvent) {
	trace, _ := ctx.Value(captureTraceKey{}).(*captureTrace)
	if trace == nil {
		return
	}
	trace.mu.Lock()
	defer trace.mu.Unlock()
	if len(trace.receipt.Events) >= captureEventLimit {
		trace.receipt.Dropped++
		return
	}
	// Callers supply fixed stage/mismatch names and authenticated node metadata,
	// never request values or err.Error(). Copy slices/pointers before retention.
	if len(event.Node) > 253 {
		event.Node = "identity_exceeds_budget"
	}
	if event.Capacity != nil {
		copy := *event.Capacity
		if len(copy.NodeUID) > 128 {
			copy.NodeUID = "identity_exceeds_budget"
		}
		copy.NodeReady = captureConditionStatus(copy.NodeReady)
		copy.MemoryPressure = captureConditionStatus(copy.MemoryPressure)
		copy.DiskPressure = captureConditionStatus(copy.DiskPressure)
		copy.PIDPressure = captureConditionStatus(copy.PIDPressure)
		copy.NetworkUnavailable = captureConditionStatus(copy.NetworkUnavailable)
		event.Capacity = &copy
	}
	event.Mismatches = append([]string(nil), event.Mismatches...)
	if event.Publication != nil {
		copy := *event.Publication
		if copy.Observed != nil {
			observed := *copy.Observed
			copy.Observed = &observed
		}
		event.Publication = &copy
	}
	if event.DNSAuthority != nil {
		copy := *event.DNSAuthority
		if copy.Snapshot != nil {
			snapshot := *copy.Snapshot
			copy.Snapshot = &snapshot
		}
		event.DNSAuthority = &copy
	}
	if event.DNSParent != nil {
		copy := *event.DNSParent
		event.DNSParent = &copy
	}
	trace.receipt.Events = append(trace.receipt.Events, event)
}

func captureConditionStatus(status string) string {
	switch status {
	case "", "True", "False", "Unknown":
		return status
	default:
		return "invalid"
	}
}

func (s *Server) finishCaptureTrace(trace *captureTrace, outcome string, err error, candidates, minimum int) {
	trace.mu.Lock()
	receipt := trace.receipt
	receipt.DNSRead = trace.dnsReadSnapshot()
	trace.dnsReadClosed = true
	receipt.Outcome, receipt.ErrorClass = outcome, captureErrorClass(err)
	receipt.FinishedAt = time.Now().UTC()
	receipt.Candidates, receipt.Minimum = candidates, minimum
	trace.mu.Unlock()
	s.captures.mu.Lock()
	defer s.captures.mu.Unlock()
	s.captures.completed++
	if len(s.captures.receipts) == captureReceiptLimit {
		copy(s.captures.receipts, s.captures.receipts[1:])
		s.captures.receipts = s.captures.receipts[:captureReceiptLimit-1]
		s.captures.evicted++
	}
	s.captures.receipts = append(s.captures.receipts, receipt)
	// Successful DNS/preview reads share the ordinary ring, but cannot evict
	// evidence of a real Agent request failure from this independent budget.
	if receipt.Kind == "agent-request" && err != nil {
		s.captures.agentFailureTotal++
		if len(s.captures.agentFailures) == captureReceiptLimit {
			copy(s.captures.agentFailures, s.captures.agentFailures[1:])
			s.captures.agentFailures = s.captures.agentFailures[:captureReceiptLimit-1]
			s.captures.agentFailureEvicted++
		}
		s.captures.agentFailures = append(s.captures.agentFailures, receipt)
	}
	// Frequent successful management reads must not erase the failed comparison
	// before an operator can correlate it with the exact release metadata.
	if receipt.Kind == "dns-runtime-facts" && err != nil {
		s.captures.dnsFailureTotal++
		if len(s.captures.dnsFailures) == dnsCaptureFailureLimit {
			copy(s.captures.dnsFailures, s.captures.dnsFailures[1:])
			s.captures.dnsFailures = s.captures.dnsFailures[:dnsCaptureFailureLimit-1]
			s.captures.dnsFailureEvicted++
		}
		s.captures.dnsFailures = append(s.captures.dnsFailures, receipt)
	}
}

func captureErrorClass(err error) string {
	if err == nil {
		return ""
	}
	var observation *dnsObservationReadFailure
	if errors.As(err, &observation) {
		return observation.class
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return "unexpected_eof"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if apierrors.IsUnauthorized(err) {
		return "unauthorized"
	}
	if apierrors.IsForbidden(err) {
		return "forbidden"
	}
	if apierrors.IsNotFound(err) {
		return "not_found"
	}
	if apierrors.IsTooManyRequests(err) {
		return "rate_limited"
	}
	if apierrors.IsTimeout(err) || apierrors.IsServerTimeout(err) {
		return "api_timeout"
	}
	if apierrors.IsServiceUnavailable(err) {
		return "api_unavailable"
	}
	var network net.Error
	if errors.As(err, &network) && network.Timeout() {
		return "network_timeout"
	}
	return "unavailable"
}

func captureHTTPStatus(err error) int {
	var observation *dnsObservationReadFailure
	if errors.As(err, &observation) {
		return observation.status
	}
	var kubernetes *kubernetesHTTPStatusError
	if errors.As(err, &kubernetes) {
		return kubernetes.status
	}
	var status apierrors.APIStatus
	if errors.As(err, &status) {
		return int(status.Status().Code)
	}
	return 0
}

// Preserve the existing error text while making its numeric response status
// available to metadata-only diagnostics without parsing an arbitrary body.
type kubernetesHTTPStatusError struct {
	status int
	detail error
}

func (e *kubernetesHTTPStatusError) Error() string { return e.detail.Error() }
func (e *kubernetesHTTPStatusError) Unwrap() error { return e.detail }

type dnsObservationReadFailure struct {
	class       string
	status      int
	rejection   string
	replacement *captureDNSReplacement
}

func (*dnsObservationReadFailure) Error() string { return errDNSRuntimeFacts.Error() }
func (*dnsObservationReadFailure) Unwrap() error { return errDNSRuntimeFacts }

func captureDNSRejection(err error) string {
	var observation *dnsObservationReadFailure
	if errors.As(err, &observation) {
		return observation.rejection
	}
	return ""
}

func captureDNSStateReplacement(err error) *captureDNSReplacement {
	var observation *dnsObservationReadFailure
	if errors.As(err, &observation) {
		return observation.replacement
	}
	return nil
}

// Older backends and proxies may omit these headers. Require one value from
// each fixed vocabulary; discard incomplete or malformed diagnostics without
// modifying the original error or exposing arbitrary response metadata.
func dnsObservationStateReplacement(status int, rejection string, values func(string) []string) *captureDNSReplacement {
	if status != 503 || rejection != "state_replaced" {
		return nil
	}
	single := func(header string) string {
		v := values(header)
		if len(v) == 1 {
			return v[0]
		}
		return ""
	}
	base, selection, renewal := single(dnsfacts.ReplacementBaseHeader), single(dnsfacts.ReplacementSelectionHeader), single(dnsfacts.ReplacementRenewalHeader)
	switch base {
	case dnsfacts.ReplacementCheckpoint, dnsfacts.ReplacementRetainedSource, dnsfacts.ReplacementRetainedObservation,
		dnsfacts.ReplacementRetainedFallback, dnsfacts.ReplacementRetainedBeforePending, dnsfacts.ReplacementClearFallback,
		dnsfacts.ReplacementReadinessIncomplete, dnsfacts.ReplacementRecordTransition, dnsfacts.ReplacementCandidate,
		dnsfacts.ReplacementRollback, dnsfacts.ReplacementUnattributed, dnsfacts.ReplacementUnchanged:
	default:
		return nil
	}
	switch selection {
	case dnsfacts.ReplacementBaseToBase, dnsfacts.ReplacementBaseToRenewal, dnsfacts.ReplacementRenewalToBase, dnsfacts.ReplacementRenewalToRenewal:
	default:
		return nil
	}
	if renewal != dnsfacts.ReplacementChanged && renewal != dnsfacts.ReplacementUnchanged {
		return nil
	}
	return &captureDNSReplacement{Base: base, Selection: selection, Renewal: renewal}
}

// Only source-defined DNS reason codes are retained. A proxy must not be able
// to copy arbitrary response headers into diagnostics.
func dnsObservationRejection(value string) string {
	switch value {
	case "":
		return ""
	case "state_missing", "request_cancelled", "checkpoint_validation", "route_identity", "plan_mismatch", "policy_mismatch", "checkpoint_not_positive", "node_identity", "fact_snapshot", "response_encoding", "response_size", "state_replaced", "listener_changed":
		return value
	default:
		return "unrecognized"
	}
}

func (s *Server) captureSnapshot() any {
	s.captures.mu.Lock()
	defer s.captures.mu.Unlock()
	// Completed receipts never mutate. The outer slice must be detached because
	// subsequent eviction shifts the backing array.
	return map[string]any{"schema": "fugue.capture-receipts/v1", "receipts": append([]captureReceipt(nil), s.captures.receipts...),
		"started": s.captures.sequence, "evicted": s.captures.evicted, "limit": captureReceiptLimit,
		"completed": s.captures.completed, "in_flight": s.captures.sequence - s.captures.completed,
		"agent_failures": map[string]any{"receipts": append([]captureReceipt(nil), s.captures.agentFailures...),
			"total": s.captures.agentFailureTotal, "evicted": s.captures.agentFailureEvicted, "limit": captureReceiptLimit,
			"coverage": "completed_agent_request_capture_errors_not_all_http_response_failures"},
		"dns_failures": map[string]any{"receipts": append([]captureReceipt(nil), s.captures.dnsFailures...),
			"total": s.captures.dnsFailureTotal, "evicted": s.captures.dnsFailureEvicted, "limit": dnsCaptureFailureLimit,
			"coverage": "completed_dns_runtime_fact_read_errors_not_public_dns_query_failures"},
		"traffic_failures": map[string]any{"receipts": append([]trafficDiagnosticFailure(nil), s.captures.trafficFailures...),
			"total": s.captures.trafficFailureTotal, "evicted": s.captures.trafficFailureEvicted, "limit": trafficFailureLimit,
			"coverage": "failed_published_traffic_diagnostic_reads_not_all_dns_checks_or_request_causes"},
		"consumer_failures": map[string]any{"receipts": append([]consumerRequestFailure(nil), s.captures.consumerFailures...),
			"total": s.captures.consumerFailureTotal, "evicted": s.captures.consumerFailureEvicted, "limit": consumerFailureLimit,
			"coverage": "assignment_attempt_and_heartbeat_read_or_admission_errors_including_recovered_rereads_not_http_failure_totals"},
		"coverage": "bounded_completed_local_reads_not_full_request_tracing", "authorizes_traffic": false}
}
