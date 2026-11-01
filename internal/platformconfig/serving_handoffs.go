package platformconfig

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"fugue/internal/model"
)

const ManagedServingHandoffsFact = "managed_serving_handoffs"
const ManagedHandoffReleaseReason = "release deployment image or current generation is not ready"
const ManagedHandoffOriginReason = "runtime invariant violation: current_image_mismatch"

// This is provenance for retaining an exact signed old route, NOT readiness of
// the new workload. API admission verifies it against durable business state and
// the current verified full source before signing any consumer artifact.
type ManagedServingHandoff struct {
	TenantID    string `json:"tenant_id"`
	AppID       string `json:"app_id"`
	ReleaseID   string `json:"release_id"`
	OperationID string `json:"operation_id"`
}

func ManagedServingHandoffs(snapshot RuntimeSnapshot) ([]ManagedServingHandoff, error) {
	v, present := snapshot.Facts[ManagedServingHandoffsFact]
	if !present {
		return nil, nil
	}
	raw, err := json.Marshal(v)
	if err != nil || len(raw) > 2<<20 {
		return nil, fmt.Errorf("managed handoff facts invalid")
	}
	var out []ManagedServingHandoff
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&out) != nil || d.Decode(&struct{}{}) != io.EOF || len(out) == 0 || len(out) > 4096 {
		return nil, fmt.Errorf("managed handoff facts invalid")
	}
	seen := map[string]bool{}
	for _, h := range out {
		key := h.TenantID + "\x00" + h.AppID
		for _, id := range []string{h.TenantID, h.AppID, h.ReleaseID, h.OperationID} {
			if id == "" || id != strings.TrimSpace(id) {
				return nil, fmt.Errorf("managed handoff identity invalid")
			}
		}
		if seen[key] || !snapshot.IsolateTrafficAvailability {
			return nil, fmt.Errorf("managed handoff owner invalid")
		}
		seen[key] = true
		found := false
		for _, r := range snapshot.Releases {
			if r.ID == h.ReleaseID && r.AppID == h.AppID && r.TenantID == h.TenantID && r.Status == model.EdgeRouteStatusUnavailable && r.StatusReason == ManagedHandoffReleaseReason {
				found = true
			}
		}
		part := false
		for _, p := range snapshot.RetainedTrafficPartitions {
			if p.TenantID == h.TenantID && p.AppID == h.AppID {
				part = true
			}
		}
		if !found || !part {
			return nil, fmt.Errorf("managed handoff requires unavailable transition and retained source")
		}
	}
	return out, nil
}

func managedHandoffRetainsOrigin(route CompiledRoute, policy PolicySnapshot, snapshot RuntimeSnapshot) bool {
	if route.Status != model.EdgeRouteStatusUnavailable || route.StatusReason != ManagedHandoffOriginReason {
		return false
	}
	handoffs, err := ManagedServingHandoffs(snapshot)
	if err != nil {
		return false
	}
	for _, h := range handoffs {
		if h.TenantID != route.TenantID || h.AppID != route.AppID || route.OriginRef == "" || route.DeploymentGeneration != h.OperationID {
			continue
		}
		for _, p := range policy.TrafficConstraints {
			if p.AppID == h.AppID && p.TenantID == h.TenantID && p.Mode == model.AppTrafficModeSingle && p.StableReleaseID == h.ReleaseID && p.StableWeight == 100 && p.CandidateWeight == 0 && p.CandidateReleaseID == "" {
				return true
			}
		}
	}
	return false
}
