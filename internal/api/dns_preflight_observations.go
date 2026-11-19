package api

import (
	"time"

	"fugue/internal/model"
)

const dnsPreflightFailureLimit = 8
const dnsPreflightNodeLimit = 8

// Preflight response time identifies the exact build. Child sequences refer
// to independent, process-local rings. The node's compact failed-read witness
// survives child eviction. No extra observations are fetched.
type dnsPreflightFailure struct {
	Sequence               uint64                         `json:"sequence"`
	GeneratedAt            time.Time                      `json:"generated_at"`
	Checks                 []dnsPreflightCheckObservation `json:"checks"`
	TrafficFailureSequence uint64                         `json:"traffic_failure_sequence,omitempty"`
	Nodes                  []dnsPreflightNodeObservation  `json:"nodes,omitempty"`
	NodesOmitted           int                            `json:"nodes_omitted"`
}

type dnsPreflightCheckObservation struct {
	Name string `json:"name"`
	Pass bool   `json:"pass"`
}

type dnsPreflightNodeObservation struct {
	NodeHash       string                   `json:"node_hash"`
	Projection     string                   `json:"projection"`
	FactSequence   uint64                   `json:"fact_sequence,omitempty"`
	FactErrorClass string                   `json:"fact_error_class,omitempty"`
	FactFailure    *dnsPreflightFactFailure `json:"fact_failure,omitempty"`
	Pass           bool                     `json:"pass"`
	ProbePass      bool                     `json:"probe_pass"`
}

type dnsPreflightFactFailure struct {
	Outcome       string                     `json:"outcome"`
	EventsDropped uint64                     `json:"events_dropped,omitempty"`
	Predicate     *dnsPreflightFactPredicate `json:"predicate,omitempty"`
	Input         *dnsPreflightFactPredicate `json:"input,omitempty"`
}

type dnsPreflightFactPredicate struct {
	Stage           string            `json:"stage"`
	Rejection       string            `json:"rejection,omitempty"`
	ErrorClass      string            `json:"error_class,omitempty"`
	HTTPStatus      int               `json:"http_status,omitempty"`
	ExpectedSetHash string            `json:"expected_set_hash,omitempty"`
	ObservedSetHash string            `json:"observed_set_hash,omitempty"`
	ExpectedFence   int64             `json:"expected_fence,omitempty"`
	ObservedFence   int64             `json:"observed_fence,omitempty"`
	ObservedSource  string            `json:"observed_source,omitempty"`
	DNSParent       *captureDNSParent `json:"dns_parent,omitempty"`
}

// The receipt belongs to this read, not a lookup in either shared ring. Keep
// only the terminal predicate and its nested rejected input (if recorded),
// never copy raw node IDs, arbitrary response bodies, or whole captures.
func dnsPreflightFactFailureFromReceipt(receipt captureReceipt) *dnsPreflightFactFailure {
	failure := &dnsPreflightFactFailure{Outcome: receipt.Outcome, EventsDropped: receipt.Dropped}
	if receipt.Dropped != 0 {
		// The recorded prefix may omit the actual terminal event.
		return failure
	}
	project := func(event captureEvent) *dnsPreflightFactPredicate {
		result := &dnsPreflightFactPredicate{Stage: event.Stage, Rejection: event.Rejection, ErrorClass: event.ErrorClass, HTTPStatus: event.HTTPStatus}
		if event.DNSParent != nil {
			parent := *event.DNSParent
			result.DNSParent = &parent
		}
		if authority := event.DNSAuthority; authority != nil {
			result.ExpectedSetHash, result.ExpectedFence = authority.Expected.ExpectedSetHash, authority.Expected.FencingToken
			if authority.Snapshot != nil {
				result.ObservedSetHash, result.ObservedFence, result.ObservedSource = authority.Snapshot.ExpectedSetHash, authority.Snapshot.FencingToken, "snapshot"
			} else {
				result.ObservedSetHash, result.ObservedFence, result.ObservedSource = authority.Heartbeat.ExpectedSetHash, authority.Heartbeat.FencingToken, "heartbeat"
			}
		}
		return result
	}
	for i := len(receipt.Events) - 1; i >= 0; i-- {
		event := receipt.Events[i]
		switch {
		case event.Stage == receipt.Outcome && (event.ErrorClass != "" || event.Rejection != "" || event.HTTPStatus != 0):
			if failure.Predicate == nil {
				failure.Predicate = project(event)
			}
		case receipt.Outcome == "source" && event.Stage == "source_rejected",
			receipt.Outcome == "authority_recheck" && event.Stage == "authority_recheck_rejected",
			receipt.Outcome == "snapshot_validation" && event.Stage == "snapshot_validation_rejected":
			if failure.Predicate == nil {
				failure.Predicate = project(event)
			}
		case (receipt.Outcome == "source" || receipt.Outcome == "authority_recheck") && event.Stage == "source_assignment_rejected":
			if failure.Input == nil {
				failure.Input = project(event)
			}
		}
	}
	if failure.Predicate == nil || (failure.Predicate.Rejection != "consumer_assignment_unavailable" && failure.Predicate.Rejection != "source_unavailable") {
		failure.Input = nil
	}
	return failure
}

// Use only names explicitly emitted by the builder, once each, so future
// error messages or caller input cannot enter this local diagnostic.
var dnsPreflightCheckNames = [...]string{
	"dns_delegation_configuration", "dns_inventory", "current_parent_ns",
	"dns_node_count", "healthy_dns_nodes", "dns_bundle_version_stable",
	"route_dns_invariant", "edge_tls_ready", "cache_errors_zero",
	"kubernetes_health_gate", "dns_reachability_and_probe",
}

func dnsPreflightNodeReceipt(node model.DNSDelegationNodeCheck, reads map[string]dnsInventoryFactRead) dnsPreflightNodeObservation {
	r := dnsPreflightNodeObservation{NodeHash: captureIdentityHash(node.PhysicalNodeID),
		Pass: node.Pass, ProbePass: node.ProbePass, Projection: "inventory_only"}
	if node.ServingObservation == nil {
		return r
	}
	read, found := reads[node.PhysicalNodeID]
	r.FactSequence = read.facts.captureSequence
	switch {
	case !found:
		r.Projection = "not_dispatched"
	case read.err != nil:
		r.Projection = "read_failed"
		r.FactErrorClass = captureErrorClass(read.err)
		r.FactFailure = read.facts.preflightFailure
	case node.ServingObservation.State == "ready":
		r.Projection = "ready"
	case node.ServingObservation.State == "not_ready":
		r.Projection = "not_ready"
	default:
		r.Projection = "read_not_projected"
	}
	return r
}

func (s *Server) retainDNSPreflightObservation(response model.DNSDelegationPreflightResponse, reads map[string]dnsInventoryFactRead, trafficFailureSequence uint64) {
	var r dnsPreflightFailure
	if !response.Pass {
		r.GeneratedAt, r.TrafficFailureSequence = response.GeneratedAt, trafficFailureSequence
		seen := [len(dnsPreflightCheckNames)]bool{}
		for _, check := range response.Checks {
			for i, name := range dnsPreflightCheckNames {
				if check.Name == name && !seen[i] {
					r.Checks = append(r.Checks, dnsPreflightCheckObservation{Name: name, Pass: check.Pass})
					seen[i] = true
					break
				}
			}
		}
		n := min(dnsPreflightNodeLimit, len(response.Nodes))
		r.NodesOmitted = len(response.Nodes) - n
		for _, node := range response.Nodes[:n] {
			r.Nodes = append(r.Nodes, dnsPreflightNodeReceipt(node, reads))
		}
	}
	s.captures.mu.Lock()
	defer s.captures.mu.Unlock()
	s.captures.dnsPreflightCompleted++
	if response.Pass {
		return
	}
	r.Sequence = s.captures.dnsPreflightCompleted
	s.captures.dnsPreflightTotal++
	if len(s.captures.dnsPreflightFailures) == dnsPreflightFailureLimit {
		copy(s.captures.dnsPreflightFailures, s.captures.dnsPreflightFailures[1:])
		s.captures.dnsPreflightFailures = s.captures.dnsPreflightFailures[:dnsPreflightFailureLimit-1]
		s.captures.dnsPreflightEvicted++
	}
	s.captures.dnsPreflightFailures = append(s.captures.dnsPreflightFailures, r)
}
