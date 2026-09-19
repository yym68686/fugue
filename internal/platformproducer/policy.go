package platformproducer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"fugue/internal/model"
)

const (
	Schema                = "fugue.platform.producer/v1"
	Scope                 = "platform-config-producer"
	Actor                 = "platform-config-producer"
	PolicyReleaseMetadata = "producer_policy_release_id"
	SourceDigestMetadata  = "producer_source_digest"
)

type Policy struct {
	SchemaVersion   string `json:"schema_version"`
	Generation      string `json:"generation"`
	Mode            string `json:"mode"`
	InputSource     string `json:"input_source"`
	TargetScope     string `json:"target_scope"`
	IntervalSeconds int    `json:"interval_seconds"`
	RefreshSeconds  int    `json:"refresh_seconds"`
}

func Decode(artifact model.PlatformArtifact) (Policy, error) {
	var p Policy
	raw, err := json.Marshal(artifact.Content)
	if err != nil {
		return p, err
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&p); err != nil {
		return p, fmt.Errorf("producer policy schema: %w", err)
	}
	if artifact.ArtifactKind != model.PlatformArtifactKindPolicySnapshot || artifact.ScopeKey != Scope || p.SchemaVersion != Schema || strings.TrimSpace(p.Generation) == "" || p.Generation != artifact.Generation {
		return p, fmt.Errorf("producer policy identity or scope invalid")
	}
	if p.Mode != "paused" && p.Mode != "shadow" || p.InputSource != "business-migration" || p.TargetScope != "global" || p.IntervalSeconds < 30 || p.IntervalSeconds > 900 || p.RefreshSeconds < 120 || p.RefreshSeconds > 3600 || p.RefreshSeconds < p.IntervalSeconds {
		return p, fmt.Errorf("producer policy mode, source or schedule invalid")
	}
	return p, nil
}
