package model

import (
	"encoding/hex"
	"errors"
	"math"
	"net"
	"strconv"
	"strings"
	"time"
)

type EdgeNetworkSample struct {
	ID            string    `json:"id"`
	EdgeID        string    `json:"edge_id"`
	EdgeGroupID   string    `json:"edge_group_id"`
	Hostname      string    `json:"hostname"`
	PathPrefix    string    `json:"path_prefix"`
	TrafficClass  string    `json:"traffic_class"`
	RouteDigest   string    `json:"route_digest"`
	BundleVersion string    `json:"bundle_version"`
	ServiceTarget string    `json:"service_target"`
	Source        string    `json:"source"`
	ServiceRTTMS  *float64  `json:"service_rtt_ms"`
	ObservedAt    time.Time `json:"observed_at"`
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
		sample.BundleVersion != "" && len(sample.BundleVersion) <= 256 && targetErr == nil && portErr == nil && portNumber > 0 && portNumber <= 65535 &&
		strings.HasSuffix(host, ".svc.cluster.local") && !strings.ContainsAny(host, "/:@?# \t\r\n") &&
		len(sample.ServiceTarget) <= 512 && sample.Source == "service_endpoint_tcp_info_v1" &&
		!sample.ObservedAt.IsZero()
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
