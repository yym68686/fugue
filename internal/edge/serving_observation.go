package edge

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net"
	"slices"
	"sync"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconsumer"
)

const servingObservationLimit = 24
const servingProbeObservationLimit = 8

// These observations describe existing checks, never authorize serving. Keep
// failures separately from the latest attempt so a recovery cannot erase the
// cause of a negative heartbeat. Only the owner-only Unix endpoint reads them.
type servingObservations struct {
	mu       sync.Mutex
	total    uint64
	failed   uint64
	evicted  uint64
	latest   servingObservation
	failures []servingObservation
}

type servingPublicationObservation struct {
	ReleaseSet string `json:"release_set_hash,omitempty"`
	Route      string `json:"route_artifact_hash,omitempty"`
	Release    string `json:"release_hash,omitempty"`
	Channel    string `json:"channel"`
	Fence      int64  `json:"fencing_token"`
}

type servingProbeFailure struct {
	Identity string `json:"identity_hash"`
	Reason   string `json:"reason"`
}

type servingProbeObservation struct {
	Total    int                   `json:"total"`
	Failed   int                   `json:"failed"`
	Failures []servingProbeFailure `json:"failures,omitempty"`
	Omitted  int                   `json:"failures_omitted"`
}

type servingNegativeObservation struct {
	Stage     string                                      `json:"stage,omitempty"`
	Sequence  int64                                       `json:"heartbeat_sequence,omitempty"`
	Attempted int                                         `json:"attempted"`
	Accepted  int                                         `json:"accepted"`
	Error     string                                      `json:"error_class,omitempty"`
	Request   *platformconsumer.RequestFailureObservation `json:"request_failure,omitempty"`
}

type servingObservation struct {
	Sequence   uint64                                      `json:"sequence"`
	StartedAt  time.Time                                   `json:"started_at"`
	FinishedAt time.Time                                   `json:"finished_at"`
	Stage      string                                      `json:"stage"`
	Outcome    string                                      `json:"outcome"`
	Error      string                                      `json:"error_class,omitempty"`
	Cause      string                                      `json:"cause_class,omitempty"`
	Bundle     string                                      `json:"bundle_hash,omitempty"`
	Applied    servingPublicationObservation               `json:"applied"`
	Expected   servingPublicationObservation               `json:"expected"`
	Reused     bool                                        `json:"reused_probes"`
	Routes     servingProbeObservation                     `json:"routes"`
	TLS        servingProbeObservation                     `json:"tls"`
	Negative   servingNegativeObservation                  `json:"negative"`
	Request    *platformconsumer.RequestFailureObservation `json:"request_failure,omitempty"`
}

func servingIdentityHash(value string) string {
	if value == "" {
		return ""
	}
	if len(value) > 4096 {
		return "identity_exceeds_budget"
	}
	sum := sha256.Sum256([]byte(value))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func servingPublication(binding *model.TrafficReleaseBinding) servingPublicationObservation {
	if binding == nil {
		return servingPublicationObservation{}
	}
	channel := binding.ReleaseChannel
	switch channel {
	case "shadow", "gray", "full":
	default:
		channel = "unrecognized"
	}
	return servingPublicationObservation{ReleaseSet: servingIdentityHash(binding.ReleaseSetID), Route: servingIdentityHash(binding.RouteArtifactID), Release: servingIdentityHash(binding.ReleaseID), Channel: channel, Fence: binding.FencingToken}
}

// Never return arbitrary error text, which can contain hosts, paths or tokens.
// Exact comparisons intentionally leave newly introduced failures unclassified.
func servingErrorClass(err error) string {
	if err == nil {
		return ""
	}
	switch {
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, errServingTrafficReleaseMismatch):
		return "publication_mismatch"
	case errors.Is(err, errServingBundleChanged):
		return "bundle_changed"
	case errors.Is(err, platformconsumer.ErrAssignmentChanged):
		return "assignment_changed"
	case errors.Is(err, platformconsumer.ErrNoServingAssignment):
		return "no_serving_assignment"
	case platformconsumer.IsUnavailable(err):
		return "platform_unavailable"
	}
	var networkError net.Error
	if errors.As(err, &networkError) && networkError.Timeout() {
		return "deadline"
	}
	switch err.Error() {
	case "traffic TLS readiness is incomplete":
		return "tls_incomplete"
	case "traffic local HTTPS route probe failed":
		return "local_https_failed"
	case "traffic route proof mismatch":
		return "route_proof_mismatch"
	case "placement route proof invalid":
		return "route_proof_invalid"
	case "traffic artifact is not the healthy applied Caddy bundle":
		return "executor_not_current"
	case "traffic serving cache unavailable":
		return "cache_unavailable"
	case "traffic serving cache differs", "traffic serving cache content differs":
		return "cache_mismatch"
	case "traffic serving projection differs", "traffic serving route differs", "traffic serving route missing":
		return "projection_mismatch"
	case "traffic TLS evidence expired before report", "traffic route evidence expired before report":
		return "evidence_expired"
	case "TLS readiness serving bundle changed":
		return "tls_bundle_changed"
	case "traffic serving activation changed", "traffic serving activation changed before report", "traffic failure observation activation or bundle changed":
		return "activation_changed"
	case "traffic serving receipt acknowledgement mismatch":
		return "acknowledgement_mismatch"
	default:
		return "unclassified"
	}
}

func (o *servingProbeObservation) failure(identity, reason string) {
	o.Failed++
	if len(o.Failures) < servingProbeObservationLimit {
		o.Failures = append(o.Failures, servingProbeFailure{Identity: servingIdentityHash(identity), Reason: reason})
	} else {
		o.Omitted++
	}
}

func servingTLSObservation(receipt *platformTLSReadinessReceipt, version string, now time.Time) servingProbeObservation {
	var observation servingProbeObservation
	if receipt == nil {
		return observation
	}
	observation.Total = len(receipt.Facts)
	for _, fact := range receipt.Facts {
		if summarizePlatformTLSReadiness(&platformTLSReadinessReceipt{BundleVersion: receipt.BundleVersion, BundleValidUntil: receipt.BundleValidUntil, Facts: []platformTLSFact{fact}}, version, now).ReadyProbes == 1 {
			continue
		}
		reason := fact.Reason
		switch reason {
		case "hostname_not_authorized_by_serving_bundle", "tls_handshake_failed", "certificate_invalid", "serving_bundle_expired":
		default:
			reason = "freshness_or_receipt_invalid"
		}
		observation.failure(fact.Hostname, reason)
	}
	return observation
}

func cloneServingObservation(value servingObservation) servingObservation {
	value.Routes.Failures = slices.Clone(value.Routes.Failures)
	value.TLS.Failures = slices.Clone(value.TLS.Failures)
	if value.Request != nil {
		copy := *value.Request
		value.Request = &copy
	}
	if value.Negative.Request != nil {
		copy := *value.Negative.Request
		value.Negative.Request = &copy
	}
	return value
}

func (s *Service) retainServingObservation(value servingObservation, result error) {
	value = cloneServingObservation(value)
	value.FinishedAt = time.Now().UTC()
	value.Error = servingErrorClass(result)
	if result != nil {
		value.Outcome = "failed"
		if servingObservationChanged(result) {
			value.Outcome = "awaiting_release"
		} else if errors.Is(result, context.Canceled) {
			value.Outcome = "canceled"
		}
	}
	o := &s.servingObservations
	o.mu.Lock()
	defer o.mu.Unlock()
	o.total++
	value.Sequence = o.total
	o.latest = value
	if result == nil {
		return
	}
	o.failed++
	if len(o.failures) == servingObservationLimit {
		copy(o.failures, o.failures[1:])
		o.failures = o.failures[:servingObservationLimit-1]
		o.evicted++
	}
	o.failures = append(o.failures, value)
}

func (s *Service) ServingObservationSnapshot(context.Context) (any, error) {
	o := &s.servingObservations
	o.mu.Lock()
	defer o.mu.Unlock()
	failures := make([]servingObservation, len(o.failures))
	for i, value := range o.failures {
		failures[i] = cloneServingObservation(value)
	}
	return struct {
		Coverage string               `json:"coverage"`
		Total    uint64               `json:"completed"`
		Failed   uint64               `json:"failed"`
		Evicted  uint64               `json:"evicted"`
		Limit    int                  `json:"limit"`
		Latest   servingObservation   `json:"latest"`
		Failures []servingObservation `json:"failures"`
	}{"completed_serving_checks_since_process_start_not_historical_reconstruction", o.total, o.failed, o.evicted, servingObservationLimit, cloneServingObservation(o.latest), failures}, nil
}
