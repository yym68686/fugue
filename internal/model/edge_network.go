package model

import (
	"encoding/hex"
	"errors"
	"math"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"time"
)

type EdgeNetworkSample struct {
	ServiceConnectFailed *bool                    `json:"service_connect_failed,omitempty"`
	RouteWitness         *EdgeNetworkRouteWitness `json:"route_witness,omitempty"`
	ClientNetwork        *EdgeClientNetworkSample `json:"client_network,omitempty"`
	ID                   string                   `json:"id"`
	EdgeID               string                   `json:"edge_id"`
	EdgeGroupID          string                   `json:"edge_group_id"`
	Hostname             string                   `json:"hostname"`
	PathPrefix           string                   `json:"path_prefix"`
	TrafficClass         string                   `json:"traffic_class"`
	RouteDigest          string                   `json:"route_digest"`
	BundleVersion        string                   `json:"bundle_version"`
	ServiceTarget        string                   `json:"service_target"`
	Source               string                   `json:"source"`
	ServiceRTTMS         *float64                 `json:"service_rtt_ms"`
	ObservedAt           time.Time                `json:"observed_at"`
}

func EdgeNetworkServiceSource(source string) bool {
	return source == "service_endpoint_tcp_info_v1" || source == "service_endpoint_tcp_probe_v1"
}

type EdgeNetworkRouteWitness struct {
	Address      string                   `json:"address"`
	ValidUntil   time.Time                `json:"valid_until"`
	NodeCapacity *EdgeNetworkNodeCapacity `json:"node_capacity,omitempty"`
}

type EdgeNetworkNodeCapacity struct {
	Source                   string    `json:"source"`
	NodeUID                  string    `json:"node_uid"`
	ObservedAt               time.Time `json:"observed_at"`
	ValidUntil               time.Time `json:"valid_until"`
	CPUObservedAt            time.Time `json:"cpu_observed_at"`
	MemoryObservedAt         time.Time `json:"memory_observed_at"`
	CPUUsageNanoCores        uint64    `json:"cpu_usage_nanocores"`
	CPUAllocatableMilliCores int64     `json:"cpu_allocatable_millicores"`
	MemoryWorkingSetBytes    uint64    `json:"memory_working_set_bytes"`
	MemoryAllocatableBytes   int64     `json:"memory_allocatable_bytes"`
	Pressure                 []string  `json:"pressure"`
}

func ValidateEdgeNetworkNodeCapacity(capacity *EdgeNetworkNodeCapacity) error {
	if capacity == nil || capacity.Source != "kubelet_node_allocatable_v1" || capacity.NodeUID == "" || len(capacity.NodeUID) > 253 ||
		strings.ContainsAny(capacity.NodeUID, " \t\r\n\x00") || capacity.CPUObservedAt.IsZero() || capacity.MemoryObservedAt.IsZero() ||
		capacity.CPUAllocatableMilliCores <= 0 || capacity.MemoryAllocatableBytes <= 0 {
		return errors.New("invalid node capacity identity or denominator")
	}
	oldest := capacity.CPUObservedAt
	if capacity.MemoryObservedAt.Before(oldest) {
		oldest = capacity.MemoryObservedAt
	}
	if !capacity.ObservedAt.Equal(oldest) || !capacity.ValidUntil.Equal(oldest.Add(2*time.Minute)) ||
		!capacity.CPUObservedAt.Before(capacity.ValidUntil) || !capacity.MemoryObservedAt.Before(capacity.ValidUntil) {
		return errors.New("invalid node capacity observation times")
	}
	seen := map[string]bool{}
	for _, condition := range capacity.Pressure {
		if (condition != "MemoryPressure" && condition != "DiskPressure" && condition != "PIDPressure") || seen[condition] {
			return errors.New("invalid node capacity pressure state")
		}
		seen[condition] = true
	}
	return nil
}

func EdgeNetworkNodeUtilization(capacity *EdgeNetworkNodeCapacity) (float64, error) {
	if err := ValidateEdgeNetworkNodeCapacity(capacity); err != nil {
		return 0, err
	}
	value := math.Max(float64(capacity.CPUUsageNanoCores)/1e6/float64(capacity.CPUAllocatableMilliCores), float64(capacity.MemoryWorkingSetBytes)/float64(capacity.MemoryAllocatableBytes))
	if len(capacity.Pressure) != 0 {
		value = math.Max(1, value)
	}
	return math.Min(1, value), nil
}

func EdgeNetworkWitnessMatches(sample, witness EdgeNetworkSample) bool {
	if ValidateEdgeNetworkSample(sample) != nil || ValidateEdgeNetworkSample(witness) != nil ||
		(!EdgeNetworkServiceSource(sample.Source) && sample.Source != "public_front_tcp_info_v1") || witness.Source != "route_tls_witness_v1" ||
		sample.EdgeID != witness.EdgeID || sample.EdgeGroupID != witness.EdgeGroupID || sample.Hostname != witness.Hostname ||
		sample.PathPrefix != witness.PathPrefix || sample.TrafficClass != witness.TrafficClass ||
		sample.RouteDigest != witness.RouteDigest || sample.BundleVersion != witness.BundleVersion {
		return false
	}
	return !sample.ObservedAt.Before(witness.ObservedAt.Add(-2*time.Minute)) &&
		!sample.ObservedAt.After(witness.ObservedAt.Add(2*time.Minute)) && sample.ObservedAt.Before(witness.RouteWitness.ValidUntil)
}

func ValidateEdgeNetworkSample(sample EdgeNetworkSample) error {
	digest, digestErr := hex.DecodeString(strings.TrimPrefix(sample.RouteDigest, "sha256:"))
	host, port, targetErr := net.SplitHostPort(sample.ServiceTarget)
	portNumber, portErr := strconv.Atoi(port)
	valid := sample.ID != "" && len(sample.ID) <= 128 && sample.EdgeID != "" && len(sample.EdgeID) <= 128 &&
		sample.EdgeGroupID != "" && len(sample.EdgeGroupID) <= 128 && sample.Hostname != "" && len(sample.Hostname) <= 253 &&
		!strings.ContainsAny(sample.Hostname, "/:@?# \t\r\n") && sample.Hostname == strings.TrimSuffix(strings.ToLower(sample.Hostname), ".") &&
		strings.HasPrefix(sample.PathPrefix, "/") && len(sample.PathPrefix) <= 2048 &&
		digestErr == nil && len(digest) == 32 && strings.HasPrefix(sample.RouteDigest, "sha256:") && len(sample.RouteDigest) == 71 &&
		sample.BundleVersion != "" && len(sample.BundleVersion) <= 256 &&
		!sample.ObservedAt.IsZero()
	switch sample.Source {
	case "service_endpoint_tcp_info_v1", "service_endpoint_tcp_probe_v1":
		valid = valid && sample.RouteWitness == nil && sample.ClientNetwork == nil && targetErr == nil && portErr == nil && portNumber > 0 && portNumber <= 65535 &&
			strings.HasSuffix(host, ".svc.cluster.local") && !strings.ContainsAny(host, "/:@?# \t\r\n") && len(sample.ServiceTarget) <= 512
		if sample.Source == "service_endpoint_tcp_probe_v1" {
			valid = valid && sample.ServiceConnectFailed != nil && (!*sample.ServiceConnectFailed || sample.ServiceRTTMS == nil)
		}
	case "public_front_tcp_info_v1":
		valid = valid && sample.RouteWitness == nil && sample.ServiceTarget == "" && sample.ServiceRTTMS == nil && ValidateEdgeClientNetworkSample(sample.ClientNetwork) == nil
		if sample.ClientNetwork != nil {
			valid = valid && sample.ObservedAt.Equal(sample.ClientNetwork.ObservedAt)
		}
	case "route_tls_witness_v1":
		valid = valid && sample.RouteWitness != nil && sample.ClientNetwork == nil && sample.ServiceTarget == "" && sample.ServiceRTTMS == nil
		if witness := sample.RouteWitness; witness != nil {
			address, err := netip.ParseAddr(witness.Address)
			valid = valid && err == nil && address.IsGlobalUnicast() && !address.IsPrivate() && !address.Is4In6() &&
				witness.Address == address.String() && witness.ValidUntil.After(sample.ObservedAt)
			if witness.NodeCapacity != nil {
				valid = valid && ValidateEdgeNetworkNodeCapacity(witness.NodeCapacity) == nil
			}
		}
	default:
		valid = false
	}
	if sample.Source != "service_endpoint_tcp_probe_v1" && sample.ServiceConnectFailed != nil {
		valid = false
	}
	switch sample.TrafficClass {
	case "streaming", "dynamic_api":
	default:
		valid = false
	}
	if sample.ServiceRTTMS != nil {
		value := *sample.ServiceRTTMS
		valid = valid && value >= 0 && value <= 60000 && !math.IsNaN(value) && !math.IsInf(value, 0)
	}
	if !valid {
		return errors.New("invalid edge network observation")
	}
	return nil
}

type EdgeClientNetworkSample struct {
	ConnectionID          string                    `json:"connection_id"`
	Slot                  string                    `json:"slot"`
	Scope                 string                    `json:"scope"`
	StartedAt             time.Time                 `json:"started_at"`
	ObservedAt            time.Time                 `json:"observed_at"`
	TCPInfoAvailable      bool                      `json:"tcp_info_available"`
	RTTMS                 *float64                  `json:"rtt_ms"`
	MinRTTMS              *float64                  `json:"min_rtt_ms"`
	RTTVarianceMS         *float64                  `json:"rtt_variance_ms"`
	SegmentsOut           uint32                    `json:"segments_out"`
	RetransmittedSegments uint32                    `json:"retransmitted_segments"`
	BytesSent             uint64                    `json:"bytes_sent"`
	BytesRetransmitted    uint64                    `json:"bytes_retransmitted"`
	Backend               *EdgeClientNetworkBackend `json:"backend,omitempty"`
}

type EdgeClientNetworkBackend struct {
	Namespace       string `json:"namespace"`
	PodName         string `json:"pod_name"`
	PodUID          string `json:"pod_uid"`
	PodVersion      string `json:"pod_version"`
	ServiceName     string `json:"service_name"`
	ServiceUID      string `json:"service_uid"`
	ServiceVersion  string `json:"service_version"`
	EndpointsDigest string `json:"endpoints_digest"`
}

func ValidateEdgeClientNetworkSample(sample *EdgeClientNetworkSample) error {
	if sample == nil || sample.ConnectionID == "" || len(sample.ConnectionID) > 128 || strings.ContainsAny(sample.ConnectionID, " \t\r\n\x00") ||
		(sample.Slot != "a" && sample.Slot != "b") || sample.StartedAt.IsZero() || sample.ObservedAt.Before(sample.StartedAt) {
		return errors.New("invalid public Front connection observation")
	}
	if backend := sample.Backend; backend != nil {
		for _, value := range []string{backend.Namespace, backend.PodName, backend.PodUID, backend.PodVersion, backend.ServiceName, backend.ServiceUID, backend.ServiceVersion} {
			if value == "" || len(value) > 253 || strings.ContainsAny(value, " \t\r\n\x00/\\") {
				return errors.New("invalid public Front backend identity")
			}
		}
		digest, err := hex.DecodeString(strings.TrimPrefix(backend.EndpointsDigest, "sha256:"))
		if err != nil || len(digest) != 32 || len(backend.EndpointsDigest) != 71 || !strings.HasPrefix(backend.EndpointsDigest, "sha256:") {
			return errors.New("invalid public Front endpoint identity")
		}
	}
	prefix, err := netip.ParsePrefix(strings.TrimPrefix(sample.Scope, "tcp_peer:"))
	if err != nil || !strings.HasPrefix(sample.Scope, "tcp_peer:") || prefix != prefix.Masked() ||
		(prefix.Addr().Is4() && prefix.Bits() != 24) || (!prefix.Addr().Is4() && prefix.Bits() != 48) ||
		!prefix.Addr().IsGlobalUnicast() || prefix.Addr().IsPrivate() || prefix.Addr().Is4In6() {
		return errors.New("invalid coarse public TCP peer scope")
	}
	for _, value := range []*float64{sample.RTTMS, sample.MinRTTMS, sample.RTTVarianceMS} {
		if value != nil && (*value < 0 || *value > 60000 || math.IsNaN(*value) || math.IsInf(*value, 0)) {
			return errors.New("invalid public Front RTT")
		}
	}
	if !sample.TCPInfoAvailable && (sample.RTTMS != nil || sample.MinRTTMS != nil || sample.RTTVarianceMS != nil || sample.SegmentsOut != 0 || sample.RetransmittedSegments != 0 || sample.BytesSent != 0 || sample.BytesRetransmitted != 0) {
		return errors.New("unavailable TCP_INFO contains measurements")
	}
	return nil
}
