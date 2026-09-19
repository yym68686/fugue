package platformconfig

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"

	"fugue/internal/model"
)

type TrafficRolloutCohort struct {
	ID           string   `json:"id"`
	EdgeGroupIDs []string `json:"edge_group_ids"`
}

var trafficCohortIDPattern = regexp.MustCompile(`^[a-z0-9]+(?:-[a-z0-9]+)*$`)

func NormalizeTrafficRolloutCohorts(in []TrafficRolloutCohort) []TrafficRolloutCohort {
	if len(in) == 0 {
		return nil
	}
	out := append([]TrafficRolloutCohort(nil), in...)
	for i := range out {
		out[i].EdgeGroupIDs = append([]string(nil), in[i].EdgeGroupIDs...)
		sort.Strings(out[i].EdgeGroupIDs)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func ValidateTrafficRolloutCohorts(cohorts []TrafficRolloutCohort) error {
	if len(cohorts) > 65 {
		return fmt.Errorf("too many traffic rollout cohorts")
	}
	ids := map[string]bool{}
	for _, c := range cohorts {
		if len(c.ID) > 128 || !trafficCohortIDPattern.MatchString(c.ID) || ids[c.ID] || len(c.EdgeGroupIDs) == 0 || len(c.EdgeGroupIDs) > 64 {
			return fmt.Errorf("invalid or duplicate traffic rollout cohort")
		}
		ids[c.ID] = true
		groups := map[string]bool{}
		for _, g := range c.EdgeGroupIDs {
			if len(g) > 128 || !platformRouteArtifactGroupID.MatchString(g) || groups[g] {
				return fmt.Errorf("invalid or duplicate traffic rollout group")
			}
			groups[g] = true
		}
	}
	return nil
}

// ResolveTrafficCanary accepts only a reference into the signed ReleaseSet's
// policy projection. It never interprets free-form labels or runtime health.
func ResolveTrafficCanary(parent model.PlatformArtifact, ref string) ([]string, error) {
	if parent.ArtifactKind != model.PlatformArtifactKindReleaseSet || !strings.HasPrefix(ref, "cohort=") {
		return nil, fmt.Errorf("traffic canary requires cohort=<id>")
	}
	id := strings.TrimPrefix(ref, "cohort=")
	if len(id) > 128 || !trafficCohortIDPattern.MatchString(id) {
		return nil, fmt.Errorf("traffic canary reference is not canonical")
	}
	var cohorts []TrafficRolloutCohort
	raw, err := json.Marshal(parent.Content["traffic_rollout_cohorts"])
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&cohorts) != nil || decoder.Decode(&struct{}{}) != io.EOF || ValidateTrafficRolloutCohorts(cohorts) != nil {
		return nil, fmt.Errorf("traffic canary policy projection invalid")
	}
	for _, c := range cohorts {
		if c.ID == id {
			return append([]string(nil), c.EdgeGroupIDs...), nil
		}
	}
	return nil, fmt.Errorf("traffic canary cohort is not declared by signed policy")
}

func TrafficCanaryContains(groups []string, group string) bool {
	for _, g := range groups {
		if g == group {
			return true
		}
	}
	return false
}

// ValidateTrafficCohortProjection binds the parent's compact selection data to
// the complete policy signed inside a child artifact. Legacy artifacts without
// cohorts remain readable, but cannot authorize a new gray TrafficReleaseSet.
func ValidateTrafficCohortProjection(parent, child model.PlatformArtifact) error {
	var policy PolicySnapshot
	raw, err := json.Marshal(child.Content["policy"])
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&policy) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return fmt.Errorf("traffic child policy invalid")
	}
	_, declared := parent.Content["traffic_rollout_cohorts"]
	if !declared && len(policy.TrafficRolloutCohorts) == 0 {
		return nil
	}
	if ValidatePolicySnapshot(policy) != nil {
		return fmt.Errorf("traffic child policy invalid")
	}
	digest, err := Digest(policy)
	if err != nil || parent.Metadata["policy_digest"] != digest || child.Metadata["policy_digest"] != digest || child.Metadata["release_set_generation"] != parent.Generation {
		return fmt.Errorf("traffic cohort policy digest or release binding differs")
	}
	raw, err = json.Marshal(parent.Content["traffic_rollout_cohorts"])
	if err != nil {
		return err
	}
	var cohorts []TrafficRolloutCohort
	decoder = json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&cohorts) != nil || decoder.Decode(&struct{}{}) != io.EOF || ValidateTrafficRolloutCohorts(cohorts) != nil {
		return fmt.Errorf("traffic cohort projection invalid")
	}
	want, _ := json.Marshal(NormalizeTrafficRolloutCohorts(policy.TrafficRolloutCohorts))
	got, _ := json.Marshal(NormalizeTrafficRolloutCohorts(cohorts))
	if !bytes.Equal(want, got) {
		return fmt.Errorf("traffic cohort projection differs from signed policy")
	}
	return nil
}
