package dnscontrol

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"sort"
	"time"

	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
)

const (
	summarySchema   = "fugue.runtime-preview-summary/v1"
	omittedEvidence = "runtime_snapshot.dns_selections[].physical_evidence"
)

// Preview is a narrow typed view of the existing runtime-summary response.
// It is never a provider witness, independent DNS response or permission to
// publish; the API has captured the inputs and omitted only named evidence.
type Preview struct {
	PreviewMode      string                         `json:"preview_mode"`
	BusinessRevision string                         `json:"business_snapshot_revision"`
	BusinessAt       time.Time                      `json:"business_snapshot_at"`
	CapturedAt       time.Time                      `json:"captured_at"`
	Intent           platformconfig.PlatformIntent  `json:"intent"`
	Policy           platformconfig.PolicySnapshot  `json:"policy"`
	Runtime          platformconfig.RuntimeSnapshot `json:"runtime_snapshot"`
	Omitted          []string                       `json:"omitted_runtime_fields"`
	Issues           []ProjectionIssue              `json:"issues"`
	Summary          *RuntimeSummary                `json:"runtime_summary"`
}

type ProjectionIssue struct {
	Code       string `json:"code"`
	Hostname   string `json:"hostname,omitempty"`
	PathPrefix string `json:"path_prefix,omitempty"`
	Reason     string `json:"reason,omitempty"`
}

type RuntimeSummary struct {
	Schema                       string `json:"schema"`
	FullSnapshotDigest           string `json:"full_snapshot_digest"`
	OmittedPhysicalEvidenceCount int    `json:"omitted_physical_evidence_count"`
	OmittedPhysicalEvidenceBytes int64  `json:"omitted_physical_evidence_bytes"`
}

type Shadow struct {
	Digest         string   `json:"digest"`
	BeforeSnapshot string   `json:"beforeSnapshot"`
	AfterSnapshot  string   `json:"afterSnapshot"`
	CandidateIDs   []string `json:"candidateIds"`
}

func validatePreviewCapture(p Preview, now time.Time) error {
	s := p.Summary
	if p.PreviewMode != "runtime_summary" || !validID(p.BusinessRevision) || p.BusinessAt.IsZero() ||
		p.CapturedAt.IsZero() || p.BusinessAt.After(p.CapturedAt) || p.CapturedAt.After(now) ||
		now.Sub(p.CapturedAt) > 5*time.Minute || now.Sub(p.BusinessAt) > 5*time.Minute ||
		p.Intent.Scope != "global" || p.Policy.Scope != "global" ||
		p.Intent.Generation == "" || p.Policy.Generation == "" ||
		p.Runtime.IntentGeneration != p.Intent.Generation || p.Runtime.PolicyGeneration != p.Policy.Generation ||
		p.Runtime.CapturedAt == nil || !p.Runtime.CapturedAt.Equal(p.CapturedAt) ||
		s == nil || s.Schema != summarySchema || !platformproducer.ValidDigest(s.FullSnapshotDigest) ||
		s.OmittedPhysicalEvidenceCount < 0 || s.OmittedPhysicalEvidenceCount > len(p.Runtime.DNSSelections) ||
		s.OmittedPhysicalEvidenceBytes < 0 || (s.OmittedPhysicalEvidenceCount == 0) != (s.OmittedPhysicalEvidenceBytes == 0) ||
		!slices.Contains(p.Omitted, omittedEvidence) {
		return errors.New("DNS shadow needs a complete, fresh runtime-summary capture")
	}
	for _, path := range p.Omitted {
		if !slices.Contains([]string{omittedEvidence, "selected_edge_group", "decision_id", "exclusion_evidence"}, path) {
			return errors.New("DNS shadow omitted unexpected runtime evidence")
		}
	}
	for _, fact := range p.Runtime.DNSSelections {
		if len(fact.PhysicalEvidence) != 0 {
			return errors.New("DNS shadow omission accounting is inconsistent")
		}
	}
	return nil
}

func comparablePolicy(p platformconfig.PolicySnapshot) platformconfig.PolicySnapshot {
	p.Generation = ""
	p.DNSAnswerRules = slices.Clone(p.DNSAnswerRules)
	for i := range p.DNSAnswerRules {
		p.DNSAnswerRules[i].SelectionMode = ""
		p.DNSAnswerRules[i].PhysicalOrder = nil
	}
	return p
}

func ValidatePreview(before, after Preview, sources Sources, next platformproducer.ProjectionPolicyInput, now time.Time) (Shadow, error) {
	for _, p := range []Preview{before, after} {
		if err := validatePreviewCapture(p, now); err != nil {
			return Shadow{}, err
		}
	}
	if before.BusinessRevision != after.BusinessRevision || !reflect.DeepEqual(before.Intent, after.Intent) ||
		!reflect.DeepEqual(comparablePolicy(before.Policy), comparablePolicy(after.Policy)) {
		return Shadow{}, errors.New("paired DNS shadow changed business inputs, unrelated compiled policy or rule ownership")
	}
	for _, issue := range after.Issues {
		if !slices.Contains(before.Issues, issue) {
			return Shadow{}, errors.New("DNS shadow introduced an unverified issue")
		}
	}
	if err := platformproducer.ValidateOfflineEdges(next.OfflineEdges); err != nil {
		return Shadow{}, err
	}
	// The API has already checked the live code identity, permanent
	// retirement and probe evidence. Reuse the same typed recovery readiness
	// evaluator over its explicitly captured fact, not a second model of
	// probe timing. This does not establish the real DNS serving outcome.
	holds := make(map[string]platformproducer.OfflineEdge, len(next.OfflineEdges))
	for _, h := range next.OfflineEdges {
		holds[h.EdgeID] = h
	}
	var recovery struct {
		CapturedAt   time.Time                                     `json:"captured_at"`
		Observations []platformproducer.OfflineRecoveryObservation `json:"observations"`
	}
	if evidence, exists := after.Runtime.Facts[platformproducer.OfflineRecoveryFact]; exists {
		raw, err := json.Marshal(evidence)
		if err != nil || json.Unmarshal(raw, &recovery) != nil ||
			recovery.CapturedAt.IsZero() || !recovery.CapturedAt.Equal(after.CapturedAt) ||
			len(recovery.Observations) == 0 || len(recovery.Observations) > len(holds) {
			return Shadow{}, errors.New("DNS shadow has invalid captured offline recovery facts")
		}
	}
	admitted := make(map[string]bool, len(recovery.Observations))
	for _, o := range recovery.Observations {
		h, ok := holds[o.EdgeID]
		if !ok || admitted[o.EdgeID] || !o.Ready(h, recovery.CapturedAt) {
			return Shadow{}, errors.New("DNS shadow contains foreign, duplicate or unverified recovery")
		}
		admitted[o.EdgeID] = true
	}
	if len(after.Runtime.DNSEdgeEndpoints) < 1 || len(after.Runtime.DNSEdgeEndpoints) > 4096 {
		return Shadow{}, errors.New("DNS shadow has no bounded candidate inventory")
	}
	var candidates []string
	seen := make(map[string]bool, len(after.Runtime.DNSEdgeEndpoints))
	heldAddresses := map[string]bool{}
	for _, h := range holds {
		if !admitted[h.EdgeID] {
			heldAddresses[h.PublicIPv4] = true
		}
	}
	for _, ep := range after.Runtime.DNSEdgeEndpoints {
		h, held := holds[ep.EdgeID]
		if !validID(ep.EdgeID) || !validID(ep.EdgeGroupID) || seen[ep.EdgeID] ||
			len(ep.A)+len(ep.AAAA) == 0 || !ep.ObservedAt.Equal(after.CapturedAt) ||
			held && (!admitted[ep.EdgeID] || h.EdgeGroupID != ep.EdgeGroupID || !slices.Contains(ep.A, h.PublicIPv4)) {
			return Shadow{}, errors.New("DNS shadow contains a held, duplicate or unbound physical endpoint")
		}
		for _, address := range ep.A {
			if heldAddresses[address] {
				return Shadow{}, errors.New("DNS shadow contains a suspended physical address under another identity")
			}
		}
		seen[ep.EdgeID] = true
		candidates = append(candidates, ep.EdgeID)
	}
	sort.Strings(candidates)
	sha, err := platformconfig.Digest(struct {
		BeforeProducer, AfterProducer ArtifactRef
		Before, After                 Preview
	}{artifactRef(sources.Producer.Artifact), artifactRef(sources.NextProducer), before, after})
	if err != nil {
		return Shadow{}, err
	}
	return Shadow{sha, before.Summary.FullSnapshotDigest, after.Summary.FullSnapshotDigest, candidates}, nil
}

// EqualChange compares the approved immutable intent fields with a fresh
// source/preview recomputation. Live observations can advance, but not the
// selected predecessor, scope, target, or rollback identity.
func EqualChange(approved, live Payload) bool {
	approved.ShadowDigest, live.ShadowDigest = "", ""
	a, ae := json.Marshal(approved)
	b, be := json.Marshal(live)
	return ae == nil && be == nil && bytes.Equal(a, b)
}
