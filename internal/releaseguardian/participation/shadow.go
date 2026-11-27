// Package participation implements Guardian's node participation policy.
package participation

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"time"

	"fugue/internal/model/edgecohort"
	"fugue/internal/productioncontrol"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	kubejson "sigs.k8s.io/json"
)

const (
	participationAnnotation  = "fugue.io/automatic-edge-participation"
	participationStateSchema = "fugue.automatic-edge-participation/v1"
	participationGroup       = "fugue.io/edge-group-id"
	participationRole        = "fugue.io/role.edge"
	participationEnrollment  = "fugue.io/edge-enrollment-phase"
)

type ParticipationPhase string
type ParticipationAction string

const (
	ParticipationActive         ParticipationPhase  = "active"
	ParticipationOffline        ParticipationPhase  = "offline"
	ParticipationRecovering     ParticipationPhase  = "recovering"
	ParticipationNone           ParticipationAction = "none"
	ParticipationHold           ParticipationAction = "record_hold_phase"
	ParticipationReviewRecovery ParticipationAction = "review_recovery"
)

type memberParticipationState struct {
	Schema string             `json:"schema"`
	Owner  string             `json:"owner"`
	Phase  ParticipationPhase `json:"phase"`
	Since  time.Time          `json:"since"`
}

// ParticipationSnapshot is an observation, not authority. ValidatedReaderBaseline
// must come from the verifier of both pinned reader rollouts, not a count of
// currently available Nodes. A nil Transport represents absence from an
// exhaustive serving transport lookup. A nonnil transport is an independent
// readback of exact node-local service identity and the DNS serving witness.
type ParticipationSnapshot struct {
	Node                    corev1.Node
	Inventory               *corev1.NodeList
	Retained                []appsv1.DaemonSet
	ValidatedReaderBaseline int
	Transport               *ParticipationTransport
}

type ParticipationTransport struct {
	Node, Group, PublicIPv4                 string
	ExternalIPs                             []string
	DNSState, DNSReason, DNSServingEvidence string
	HeartbeatFresh                          bool
}

type ParticipationPreview struct {
	Node            string                 `json:"node"`
	NodeUID         string                 `json:"nodeUid"`
	ResourceVersion string                 `json:"resourceVersion"`
	Phase           ParticipationPhase     `json:"phase"`
	Action          ParticipationAction    `json:"action"`
	RecoverySince   time.Time              `json:"recoverySince,omitempty"`
	HoldPlan        *ParticipationHoldPlan `json:"holdPlan,omitempty"`
}

// ParticipationHoldPlan describes a proposed UID/resourceVersion CAS and
// the exact annotation values it would replace. Before is rollback input,
// not permission to write it back after a different owner or version wins.
type ParticipationHoldPlan struct {
	Before map[string]string `json:"before"`
	After  map[string]string `json:"after"`
}

// PreviewParticipation ports phase selection and the owned-hold boundary.
// It cannot change a provider object or decide that recovery/readmission
// succeeded. review_recovery requires the separate Front/Worker and network
// evidence checks even if this first-stage decision agrees with legacy.
func PreviewParticipation(policy productioncontrol.ParticipationPolicy, node string, s ParticipationSnapshot, now time.Time) (ParticipationPreview, error) {
	var m productioncontrol.ParticipationMember
	for _, authorized := range policy.Members {
		if authorized.Node == node {
			if m.Node != "" {
				return ParticipationPreview{}, errors.New("ambiguous participation topology identity")
			}
			m = authorized
		}
	}
	n := s.Node
	if m.Node == "" || m.AdoptHold == nil || m.Node != n.Name || m.NodeUID == "" || m.NodeUID != string(n.UID) ||
		n.ResourceVersion == "" || n.DeletionTimestamp != nil || n.Labels[participationGroup] != m.Group ||
		n.Labels[participationRole] != "true" || now.IsZero() {
		return ParticipationPreview{}, errors.New("node does not match the physical participation enrollment")
	}
	p := ParticipationPreview{Node: n.Name, NodeUID: string(n.UID), ResourceVersion: n.ResourceVersion, Action: ParticipationNone}
	identity, err := json.Marshal(map[string]string{"node": m.Node, "node_uid": m.NodeUID, "group": m.Group, "component": m.Component})
	if err != nil {
		return ParticipationPreview{}, err
	}
	owner := digest(identity)
	var state memberParticipationState
	if raw, found := n.Annotations[participationAnnotation]; found {
		if len(raw) > 2048 || decodeStrict([]byte(raw), &state) != nil || state.Schema != participationStateSchema ||
			state.Owner != owner || state.Since.IsZero() || state.Since.After(now) ||
			(state.Phase != ParticipationActive && state.Phase != ParticipationOffline && state.Phase != ParticipationRecovering) {
			return ParticipationPreview{}, errors.New("automatic participation state belongs to a different owner or is invalid")
		}
	}
	held, err := edgecohort.ReleaseHeld(n, m.Group)
	if err != nil {
		return ParticipationPreview{}, err
	}
	if held {
		var hold edgecohort.ReleaseHold
		if decodeStrict([]byte(n.Annotations[edgecohort.ReleaseHoldAnnotation]), &hold) != nil ||
			hold.Schema != edgecohort.ReleaseHoldSchema || hold.NodeUID != m.NodeUID || hold.EdgeGroupID != m.Group ||
			hold.Reason != m.Reason || (state.Phase == "" && !*m.AdoptHold) {
			return ParticipationPreview{}, errors.New("manual or foreign release hold is retained")
		}
	}
	var ready *corev1.NodeCondition
	for i := range n.Status.Conditions {
		c := &n.Status.Conditions[i]
		if c.Type == corev1.NodeReady {
			if ready != nil || (c.Status != corev1.ConditionTrue && c.Status != corev1.ConditionFalse && c.Status != corev1.ConditionUnknown) {
				return ParticipationPreview{}, errors.New("ambiguous Node readiness")
			}
			ready = c
		}
	}
	if ready == nil {
		return ParticipationPreview{}, errors.New("missing Node readiness")
	}
	p.Phase = ParticipationActive
	if ready.Status != corev1.ConditionTrue {
		p.Phase = ParticipationOffline
	} else if t := s.Transport; t == nil {
		if held {
			p.Phase = ParticipationRecovering
		}
	} else {
		if t.Node != n.Name || t.Group != m.Group || len(t.ExternalIPs) != 1 {
			return ParticipationPreview{}, errors.New("serving transport does not match the physical member")
		}
		excluded := t.PublicIPv4 != "" && t.PublicIPv4 == t.ExternalIPs[0] && t.DNSState == "suspended" && t.DNSReason == m.Reason &&
			(t.DNSServingEvidence == "excluded_from_verified_full" || t.DNSServingEvidence == "excluded_across_full_transition")
		if held || excluded {
			p.Phase = ParticipationRecovering
			if !t.HeartbeatFresh {
				p.Phase = ParticipationOffline
			}
		}
	}
	if p.Phase == ParticipationActive {
		return p, nil
	}
	if err := verifyParticipationInventory(m, policy.Namespace, s); err != nil {
		return ParticipationPreview{}, err
	}
	switch {
	case state.Phase != p.Phase:
		p.Action = ParticipationHold
		before := maps.Clone(n.Annotations)
		after := make(map[string]string, len(before)+2)
		maps.Copy(after, before)
		// These keys retain the established wire format for existing readers.
		state, err := json.Marshal(map[string]string{"schema": participationStateSchema, "owner": owner,
			"phase": string(p.Phase), "since": now.UTC().Truncate(time.Microsecond).Format("2006-01-02T15:04:05.999999+00:00")})
		if err != nil {
			return ParticipationPreview{}, err
		}
		hold, err := json.Marshal(map[string]string{"schema": edgecohort.ReleaseHoldSchema,
			"node_uid": m.NodeUID, "edge_group_id": m.Group, "reason": m.Reason})
		if err != nil {
			return ParticipationPreview{}, err
		}
		after[participationAnnotation], after[edgecohort.ReleaseHoldAnnotation] = string(state), string(hold)
		p.HoldPlan = &ParticipationHoldPlan{Before: before, After: after}
	case p.Phase == ParticipationRecovering:
		p.RecoverySince = state.Since
		if ready.LastTransitionTime.After(p.RecoverySince) {
			p.RecoverySince = ready.LastTransitionTime.Time
		}
		if !p.RecoverySince.After(now) && now.Sub(p.RecoverySince) >= time.Duration(m.StableSeconds)*time.Second {
			p.Action = ParticipationReviewRecovery
		}
	}
	return p, nil
}

func verifyParticipationInventory(m productioncontrol.ParticipationMember, namespace string, s ParticipationSnapshot) error {
	n := s.Node
	inventory := s.Inventory
	phase := n.Labels[participationEnrollment]
	if (phase != "" && phase != "admitted") || inventory == nil ||
		inventory.Continue != "" || len(inventory.Items) > 1000 || s.ValidatedReaderBaseline < 1 {
		return errors.New("incomplete participation group or reader baseline evidence")
	}
	fixed, eligible, targetCount := 0, 0, 0
	names := make(map[string]bool, len(inventory.Items))
	for _, item := range inventory.Items {
		if item.Labels[participationRole] != "true" {
			continue
		}
		phase := item.Labels[participationEnrollment]
		if item.Name == "" || names[item.Name] || item.UID == "" || item.ResourceVersion == "" || item.DeletionTimestamp != nil ||
			item.Labels[participationGroup] != m.Group || (phase != "" && phase != "preparing" && phase != "admitted") {
			return errors.New("ambiguous participation group membership")
		}
		names[item.Name] = true
		if phase == "" {
			fixed++
		}
		_, held := item.Annotations[edgecohort.ReleaseHoldAnnotation]
		if item.Name == n.Name {
			if !reflect.DeepEqual(item.ObjectMeta, n.ObjectMeta) {
				return errors.New("node changed during participation inventory read")
			}
			targetCount++
		} else if !held && item.Labels["fugue.io/schedulable"] != "false" && phase != "preparing" {
			eligible++
		}
	}
	if targetCount != 1 || fixed != s.ValidatedReaderBaseline || eligible == 0 {
		return errors.New("participation hold would hide missing baseline or remove the last eligible member")
	}
	if s.Transport == nil {
		missing := map[string]bool{m.Front: true, m.Workers.A: true, m.Workers.B: true}
		if len(missing) != 3 || len(s.Retained) != len(missing) {
			return errors.New("incomplete retained Front and Worker inventory")
		}
		for _, workload := range s.Retained {
			if !missing[workload.Name] || workload.Namespace != namespace || workload.UID == "" || workload.DeletionTimestamp != nil ||
				workload.Spec.UpdateStrategy.Type != appsv1.OnDeleteDaemonSetStrategyType ||
				workload.Spec.Template.Labels[participationGroup] != m.Group {
				return errors.New("retained participation workload lacks exact OnDelete group ownership")
			}
			delete(missing, workload.Name)
		}
	}
	return nil
}

func digest(raw []byte) string { return fmt.Sprintf("sha256:%x", sha256.Sum256(raw)) }

func decodeStrict(raw []byte, target any) error {
	strict, err := kubejson.UnmarshalStrict(raw, target)
	if err != nil {
		return err
	}
	if len(strict) != 0 {
		return errors.New("unexpected fields in participation state")
	}
	return nil
}
