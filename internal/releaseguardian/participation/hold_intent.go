package participation

import (
	"bytes"
	"encoding/json"
	"errors"
	"maps"
	"math"
	"time"

	"fugue/internal/model/edgecohort"
	"fugue/internal/productioncontrol"
)

const holdIntentSchema = "fugue.node-participation-hold/v1"

// HoldResource identifies the one protected Node object regardless of a
// replacement UID. The physical UID is bound to the version and payload.
func HoldResource(cluster, node string) productioncontrol.Scope {
	return productioncontrol.Scope{ClusterID: cluster, ResourceID: "kubernetes/node/" + digest([]byte(node))[7:]}
}

type holdIntentState struct {
	Node        string             `json:"node"`
	UID         string             `json:"uid"`
	Group       string             `json:"group"`
	Annotations map[string]*string `json:"annotations"`
}

type holdIntentPayload struct {
	Schema             string                    `json:"schema"`
	Topology           productioncontrol.Version `json:"topology"`
	TopologyApproval   string                    `json:"topologyApproval"`
	ResourceVersion    string                    `json:"resourceVersion"`
	Phase              ParticipationPhase        `json:"phase"`
	From               holdIntentState           `json:"from"`
	To                 holdIntentState           `json:"to"`
	ShadowReportDigest string                    `json:"shadowReportDigest"`
}

// HoldStateDigest identifies the exact owned metadata of an independently
// witnessed baseline or readback. Unrelated annotation updates do not move
// the verified journal head; provider CAS still checks resourceVersion.
func HoldStateDigest(node, uid, group string, annotations map[string]string) string {
	return newHoldIntentState(node, uid, group, annotations).digest()
}

func (s holdIntentState) digest() string {
	raw, _ := json.Marshal(s)
	return digest(raw)
}

func newHoldIntentState(node, uid, group string, annotations map[string]string) holdIntentState {
	s := holdIntentState{Node: node, UID: uid, Group: group, Annotations: make(map[string]*string, 2)}
	for _, k := range []string{participationAnnotation, edgecohort.ReleaseHoldAnnotation} {
		if value, ok := annotations[k]; ok {
			s.Annotations[k] = &value
		} else {
			s.Annotations[k] = nil
		}
	}
	return s
}

// DraftHold binds a phase transition and the only two owned fields to the
// signed topology revision, exact object version and journal LKG. A draft
// neither signs the shadow report nor opens a provider write path.
func DraftHold(cluster, request, node string, topology productioncontrol.VerifiedParticipationTopology, head productioncontrol.ResourceState, shadow KubernetesShadow) (productioncontrol.Submission, error) {
	m, err := enrolledMember(topology.Policy, node)
	if err != nil || topology.Policy.Schema != productioncontrol.BoundParticipationPolicySchema ||
		topology.Policy.Kubernetes == nil || topology.Policy.Kubernetes.Validate() != nil ||
		topology.Version.Generation != topology.Policy.Generation || topology.Version.Generation < 1 ||
		!canonicalDigest(topology.Version.Digest) || !canonicalDigest(topology.ApprovalDigest) || topology.RecordedAt.IsZero() {
		return productioncontrol.Submission{}, errors.New("participation hold requires the current approved cluster-bound topology")
	}
	preview := shadow.Preview
	live := shadow.Member.Node
	if preview == nil || preview.Action != ParticipationHold || preview.HoldPlan == nil || shadow.Waiting != "" ||
		(preview.Phase != ParticipationOffline && preview.Phase != ParticipationRecovering) ||
		preview.Node != node || preview.NodeUID != m.NodeUID || live.Name != node || live.UID != m.NodeUID ||
		preview.ResourceVersion == "" || preview.ResourceVersion != live.ResourceVersion {
		return productioncontrol.Submission{}, errors.New("participation hold requires an exact live hold phase preview")
	}
	plan := preview.HoldPlan
	expected := maps.Clone(plan.Before)
	if expected == nil {
		expected = make(map[string]string, 2)
	}
	for _, k := range []string{participationAnnotation, edgecohort.ReleaseHoldAnnotation} {
		if plan.After[k] == "" || len(plan.After[k]) > 2048 || len(plan.Before[k]) > 2048 {
			return productioncontrol.Submission{}, errors.New("invalid owned participation annotation")
		}
		expected[k] = plan.After[k]
	}
	var afterState memberParticipationState
	var afterHold edgecohort.ReleaseHold
	identity, _ := json.Marshal(map[string]string{"node": m.Node, "node_uid": m.NodeUID, "group": m.Group, "component": m.Component})
	if !maps.Equal(expected, plan.After) ||
		decodeStrict([]byte(plan.After[participationAnnotation]), &afterState) != nil ||
		afterState.Schema != participationStateSchema || afterState.Owner != digest(identity) ||
		afterState.Phase != preview.Phase || afterState.Since.IsZero() || afterState.Since.Before(topology.RecordedAt) ||
		decodeStrict([]byte(plan.After[edgecohort.ReleaseHoldAnnotation]), &afterHold) != nil ||
		afterHold != (edgecohort.ReleaseHold{Schema: edgecohort.ReleaseHoldSchema, NodeUID: m.NodeUID, EdgeGroupID: m.Group, Reason: m.Reason}) {
		return productioncontrol.Submission{}, errors.New("hold proposal differs from the approved participation metadata boundary")
	}
	from := HoldStateDigest(node, m.NodeUID, m.Group, plan.Before)
	to := HoldStateDigest(node, m.NodeUID, m.Group, plan.After)
	scope := HoldResource(cluster, node)
	if head.Scope != scope || !canonicalDigest(head.EvidenceDigest) || head.Baseline.Verified.Generation < 1 ||
		!canonicalDigest(head.Baseline.Verified.Digest) || head.Pending != "" || head.LastVerified.Generation < head.Baseline.Verified.Generation ||
		head.LastVerified.Generation == math.MaxInt64 || head.LastVerified.Digest != from || from == to {
		return productioncontrol.Submission{}, errors.New("participation hold lacks a settled, exact verified predecessor")
	}
	observed, err := json.Marshal(shadow)
	if err != nil {
		return productioncontrol.Submission{}, err
	}
	payload, err := json.Marshal(holdIntentPayload{
		Schema: holdIntentSchema, Topology: topology.Version, TopologyApproval: topology.ApprovalDigest,
		ResourceVersion: live.ResourceVersion, Phase: preview.Phase,
		From: newHoldIntentState(node, m.NodeUID, m.Group, plan.Before),
		To:   newHoldIntentState(node, m.NodeUID, m.Group, plan.After), ShadowReportDigest: digest(observed),
	})
	if err != nil {
		return productioncontrol.Submission{}, err
	}
	intent, err := productioncontrol.NewIntent(cluster, scope.ResourceID, request, productioncontrol.NodeParticipation,
		head.LastVerified.Generation, from, to, from, payload)
	if err != nil {
		return productioncontrol.Submission{}, err
	}
	return productioncontrol.Submission{Intent: intent, Payload: payload}, nil
}

// InspectHold verifies the exact payload binding before making the human
// review fields visible. It does not authenticate live observations.
func InspectHold(s productioncontrol.Submission) (json.RawMessage, error) {
	if _, err := decodeHold(s); err != nil {
		return nil, err
	}
	return json.RawMessage(s.Payload), nil
}

// HoldTarget returns the exact requested Node, never a separate CLI target
// that could silently diverge from the submitted resource.
func HoldTarget(s productioncontrol.Submission) (string, error) {
	p, err := decodeHold(s)
	return p.To.Node, err
}

// PreflightHold compares a proposed change with a newly gathered shadow
// and the current LKG. A live preview generates a new timestamp, which
// does not replace the value in the proposal. A positive result is not
// execution clearance: actual hold application time, approval, authority,
// and provider CAS must still be checked separately.
func PreflightHold(s productioncontrol.Submission, topology productioncontrol.VerifiedParticipationTopology, head productioncontrol.ResourceState, live KubernetesShadow, now time.Time) error {
	p, err := decodeHold(s)
	if err != nil {
		return err
	}
	if now.IsZero() || p.Topology != topology.Version || p.TopologyApproval != topology.ApprovalDigest {
		return errors.New("participation hold topology changed since draft")
	}
	current, err := DraftHold(s.Intent.ClusterID, s.Intent.RequestID, p.To.Node, topology, head, live)
	if err != nil {
		return err
	}
	q, err := decodeHold(current)
	if err != nil {
		return err
	}
	if current.Intent.Generation != s.Intent.Generation || current.Intent.CurrentDigest != s.Intent.CurrentDigest ||
		q.ResourceVersion != p.ResourceVersion || q.Phase != p.Phase ||
		*q.To.Annotations[edgecohort.ReleaseHoldAnnotation] != *p.To.Annotations[edgecohort.ReleaseHoldAnnotation] {
		return errors.New("live participation hold version or proposed change differs from the submission")
	}
	var proposed, observed memberParticipationState
	if decodeStrict([]byte(*p.To.Annotations[participationAnnotation]), &proposed) != nil ||
		decodeStrict([]byte(*q.To.Annotations[participationAnnotation]), &observed) != nil ||
		proposed.Since.Before(topology.RecordedAt) || proposed.Since.After(observed.Since) ||
		observed.Since.After(now) || now.Sub(observed.Since) > 15*time.Second {
		return errors.New("participation hold draft or observation has an invalid transition time")
	}
	proposed.Since = observed.Since
	if proposed != observed {
		return errors.New("participation phase metadata differs from the live proposal")
	}
	return nil
}

func decodeHold(s productioncontrol.Submission) (holdIntentPayload, error) {
	var p holdIntentPayload
	i := s.Intent
	if i.Operation != productioncontrol.NodeParticipation || i.VerifyPayload(s.Payload) != nil ||
		decodeStrict(s.Payload, &p) != nil || p.Schema != holdIntentSchema ||
		p.Topology.Generation < 1 || !canonicalDigest(p.Topology.Digest) || !canonicalDigest(p.TopologyApproval) ||
		!canonicalDigest(p.ShadowReportDigest) || p.ResourceVersion == "" ||
		p.To.Node == "" || p.To.UID == "" || p.To.Group == "" ||
		p.From.Node != p.To.Node || p.From.UID != p.To.UID || p.From.Group != p.To.Group ||
		i.ResourceID != HoldResource(i.ClusterID, p.To.Node).ResourceID ||
		i.CurrentDigest != p.From.digest() || i.RollbackDigest != i.CurrentDigest ||
		i.TargetDigest != p.To.digest() || i.TargetDigest == i.CurrentDigest ||
		(p.Phase != ParticipationOffline && p.Phase != ParticipationRecovering) ||
		len(p.From.Annotations) != 2 || len(p.To.Annotations) != 2 {
		return holdIntentPayload{}, errors.New("invalid bound participation hold submission")
	}
	for _, k := range []string{participationAnnotation, edgecohort.ReleaseHoldAnnotation} {
		old, found := p.From.Annotations[k]
		next := p.To.Annotations[k]
		if !found || old != nil && len(*old) > 2048 || next == nil || *next == "" || len(*next) > 2048 {
			return holdIntentPayload{}, errors.New("invalid owned participation annotation in hold submission")
		}
	}
	var a memberParticipationState
	var h edgecohort.ReleaseHold
	if decodeStrict([]byte(*p.To.Annotations[participationAnnotation]), &a) != nil ||
		a.Schema != participationStateSchema || !canonicalDigest(a.Owner) || a.Phase != p.Phase || a.Since.IsZero() ||
		decodeStrict([]byte(*p.To.Annotations[edgecohort.ReleaseHoldAnnotation]), &h) != nil ||
		h.Schema != edgecohort.ReleaseHoldSchema || h.NodeUID != p.To.UID || h.EdgeGroupID != p.To.Group ||
		(h.Reason != "network_unavailable" && h.Reason != "quota_exhausted") {
		return holdIntentPayload{}, errors.New("invalid resulting participation hold")
	}
	encoded, err := json.Marshal(p)
	if err != nil || !bytes.Equal(encoded, s.Payload) {
		return holdIntentPayload{}, errors.New("participation hold is not canonically encoded")
	}
	return p, nil
}

func canonicalDigest(value string) bool {
	if len(value) != 71 || value[:7] != "sha256:" {
		return false
	}
	for i := 7; i < len(value); i++ {
		if c := value[i]; !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
