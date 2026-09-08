package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
)

type AppRuntimeCheck struct {
	Kind           string `json:"kind"`
	Key            string `json:"key,omitempty"`
	Pod            string `json:"pod,omitempty"`
	State          string `json:"state"`
	Source         string `json:"source"`
	DesiredSHA256  string `json:"desired_sha256,omitempty"`
	ObservedSHA256 string `json:"observed_sha256,omitempty"`
	Reason         string `json:"reason,omitempty"`
}
type AppRuntimeState struct {
	SchemaVersion     int               `json:"schema_version"`
	AppID             string            `json:"app_id"`
	Namespace         string            `json:"namespace"`
	DesiredSpecHash   string            `json:"desired_spec_hash"`
	DesiredSource     string            `json:"desired_source"`
	ObservedAt        time.Time         `json:"observed_at"`
	State             string            `json:"state"`
	ReadyPods         []string          `json:"ready_pods"`
	ServingPods       []string          `json:"endpoint_pods"`
	PodUIDs           map[string]string `json:"pod_uids"`
	Revisions         map[string]string `json:"revisions"`
	PendingOperations []string          `json:"pending_operations"`
	Checks            []AppRuntimeCheck `json:"checks"`
	MissingEvidence   []string          `json:"missing_evidence"`
}

func AppSpecSHA256(spec AppSpec) string {
	raw, err := json.Marshal(spec)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
