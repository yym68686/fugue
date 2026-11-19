package api

import (
	"time"

	"fugue/internal/model"
)

const dnsPreflightFailureLimit = 8
const dnsPreflightNodeLimit = 8

// Preflight response time identifies the exact build. Child sequences refer
// to independent, process-local rings; a missing child may have been evicted.
// No extra observations are fetched and no response field is changed.
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
	NodeHash       string `json:"node_hash"`
	Projection     string `json:"projection"`
	FactSequence   uint64 `json:"fact_sequence,omitempty"`
	FactErrorClass string `json:"fact_error_class,omitempty"`
	Pass           bool   `json:"pass"`
	ProbePass      bool   `json:"probe_pass"`
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
