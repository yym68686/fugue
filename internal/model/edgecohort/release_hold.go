package edgecohort

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"

	corev1 "k8s.io/api/core/v1"
)

// Release holds are administrator intent, independent of Kubernetes readiness,
// placement and serving configuration. A hold never changes a Pod or its LKG.
const ReleaseHoldAnnotation = "fugue.io/edge-release-hold"
const ReleaseHoldCapabilityAnnotation = "fugue.pro/edge-release-hold"
const ReleaseHoldSchema = "fugue.edge-release-hold/v1"

type ReleaseHold struct {
	Schema      string `json:"schema"`
	NodeUID     string `json:"node_uid"`
	EdgeGroupID string `json:"edge_group_id"`
	Reason      string `json:"reason"`
}

// ReleaseHeld validates an explicit hold against the currently observed Node.
// Absence of intent never infers a hold from missing or negative health.
func ReleaseHeld(node corev1.Node, group string) (bool, error) {
	raw, present := node.Annotations[ReleaseHoldAnnotation]
	if !present {
		return false, nil
	}
	fail := errors.New("Edge release hold lacks exact live node identity or supported intent")
	if len(raw) == 0 || len(raw) > 2048 || node.Name == "" || node.UID == "" || node.ResourceVersion == "" || node.DeletionTimestamp != nil || node.Labels["fugue.io/role.edge"] != "true" || node.Labels["fugue.io/edge-group-id"] != group {
		return false, fail
	}
	var hold ReleaseHold
	decoder := json.NewDecoder(bytes.NewBufferString(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&hold) != nil || decoder.Decode(new(any)) != io.EOF || hold.Schema != ReleaseHoldSchema || hold.NodeUID != string(node.UID) || hold.EdgeGroupID != group {
		return false, fail
	}
	if hold.Reason != "quota_exhausted" && hold.Reason != "network_unavailable" && hold.Reason != "maintenance" {
		return false, fail
	}
	return true, nil
}
