package platformproducer

import (
	"encoding/hex"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"
)

const CapacityRecoveryVersion = "edge-capacity-recovery-v1"
const CapacityRecoveryBodyBytes = 1 << 20

type CapacityRecoveryPolicy struct {
	Version                string   `json:"version"`
	TrafficClass           string   `json:"traffic_class"`
	ReferenceEdgeIDs       []string `json:"reference_edge_ids"`
	MinimumBytesPerSecond  int64    `json:"minimum_bytes_per_second"`
	MaxElapsedMilliseconds int64    `json:"max_elapsed_milliseconds"`
	MinimumStableSeconds   int      `json:"minimum_stable_seconds"`
}

func (p CapacityRecoveryPolicy) Validate() error {
	if p.Version != CapacityRecoveryVersion || p.TrafficClass != "streaming" || len(p.ReferenceEdgeIDs) < 1 || len(p.ReferenceEdgeIDs) > 3 || p.MinimumBytesPerSecond < 65536 || p.MinimumBytesPerSecond > 16777216 || p.MaxElapsedMilliseconds < 1000 || p.MaxElapsedMilliseconds > 15000 || p.MinimumStableSeconds < 300 || p.MinimumStableSeconds > 7200 {
		return fmt.Errorf("bounded explicit capacity recovery policy required")
	}
	seen := map[string]bool{}
	for _, id := range p.ReferenceEdgeIDs {
		if !offlineIdentity.MatchString(id) || seen[id] {
			return fmt.Errorf("unique capacity reference identity required")
		}
		seen[id] = true
	}
	return nil
}

// Evidence summarizes a completely verified signed round. The full report and
// physical code observation are retained separately by digest. Neither this
// object nor the report is serving configuration or permission to remove a hold.
type CapacityRecoveryEvidence struct {
	CheckedAt           time.Time            `json:"checked_at"`
	ReportDigest        string               `json:"report_digest"`
	ReferenceEdgeID     string               `json:"reference_edge_id"`
	Bytes               int                  `json:"bytes"`
	ElapsedMilliseconds float64              `json:"elapsed_milliseconds"`
	BytesPerSecond      float64              `json:"bytes_per_second"`
	Code                CapacityCodeIdentity `json:"code"`
	RouteDigest         string               `json:"route_digest"`
}

func (e *CapacityRecoveryEvidence) Passes(p CapacityRecoveryPolicy, edgeID string) bool {
	if e == nil || p.Validate() != nil || e.CheckedAt.IsZero() || !ValidDigest(e.ReportDigest) || !ValidDigest(e.RouteDigest) || e.ReferenceEdgeID == edgeID || !slices.Contains(p.ReferenceEdgeIDs, e.ReferenceEdgeID) || e.Bytes != CapacityRecoveryBodyBytes || e.ElapsedMilliseconds <= 0 || math.IsNaN(e.ElapsedMilliseconds) || math.IsInf(e.ElapsedMilliseconds, 0) || e.ElapsedMilliseconds > float64(p.MaxElapsedMilliseconds) || math.IsNaN(e.BytesPerSecond) || math.IsInf(e.BytesPerSecond, 0) || e.BytesPerSecond < float64(p.MinimumBytesPerSecond) {
		return false
	}
	if !e.Code.Valid() {
		return false
	}
	want := float64(e.Bytes) * 1000 / e.ElapsedMilliseconds
	return math.Abs(want-e.BytesPerSecond) <= 0.000001*want
}

// Physical identities are observed from the selected public transport and its
// exact Worker target, not inferred from a heartbeat or a group member count.
type CapacityCodeIdentity struct {
	NodeUID               string `json:"node_uid"`
	FrontPodUID           string `json:"front_pod_uid"`
	WorkerPodUID          string `json:"worker_pod_uid"`
	ServiceUID            string `json:"service_uid"`
	AuthorityUID          string `json:"authority_uid"`
	AuthorityRecordDigest string `json:"authority_record_digest"`
	WorkerSourceSHA       string `json:"worker_source_sha"`
	WorkerImageDigest     string `json:"worker_image_digest"`
	FrontImageDigest      string `json:"front_image_digest"`
	ProxyImageDigest      string `json:"proxy_image_digest"`
	Slot                  string `json:"slot"`
	Fence                 uint64 `json:"fence"`
	Bundle                string `json:"bundle"`
}

func (c CapacityCodeIdentity) Valid() bool {
	for _, id := range []string{c.NodeUID, c.FrontPodUID, c.WorkerPodUID, c.ServiceUID, c.AuthorityUID} {
		if len(id) < 1 || len(id) > 128 || strings.ContainsAny(id, " \t\r\n\x00") {
			return false
		}
	}
	source, err := hex.DecodeString(c.WorkerSourceSHA)
	return err == nil && len(source) == 20 && strings.ToLower(c.WorkerSourceSHA) == c.WorkerSourceSHA && ValidDigest(c.AuthorityRecordDigest) && ValidDigest(c.WorkerImageDigest) && ValidDigest(c.FrontImageDigest) && ValidDigest(c.ProxyImageDigest) && (c.Slot == "a" || c.Slot == "b") && c.Fence > 0 && c.Fence != ^uint64(0) && len(c.Bundle) > 0 && len(c.Bundle) <= 512 && !strings.ContainsAny(c.Bundle, " \t\r\n\x00")
}

func sameCapacityCode(a, b *CapacityRecoveryEvidence) bool {
	return a != nil && b != nil && a.Code == b.Code && a.RouteDigest == b.RouteDigest
}
