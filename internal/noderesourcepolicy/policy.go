// Package noderesourcepolicy defines configuration intent for node resources.
// The node persists this document independently of updater code releases.
package noderesourcepolicy

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

const TaskType = "reconcile-host-inotify-policy"
const UpdaterVersion = "v47"

type Policy struct {
	Schema                string `json:"schema"`
	Enabled               bool   `json:"enabled"`
	MinInstances          int    `json:"min_instances"`
	MaxInstances          int    `json:"max_instances"`
	InstancesPerContainer int    `json:"instances_per_container"`
	BaseInstances         int    `json:"base_instances"`
	HeadroomPercent       int    `json:"headroom_percent"`
	MinWatches            int    `json:"min_watches"`
	MaxWatches            int    `json:"max_watches"`
	WatchesPerContainer   int    `json:"watches_per_container"`
	WarningPercent        int    `json:"warning_percent"`
}

func Parse(raw string) (Policy, error) {
	var p Policy
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&p); err != nil {
		return p, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return p, fmt.Errorf("policy must be one JSON document")
	}
	var fields map[string]json.RawMessage
	_ = json.Unmarshal([]byte(raw), &fields)
	if _, ok := fields["enabled"]; !ok || string(fields["enabled"]) == "null" {
		return p, fmt.Errorf("policy enabled must be an explicit boolean")
	}
	if p.Schema != "fugue.node-resource-policy/v1" ||
		p.MinInstances < 128 || p.MaxInstances < p.MinInstances || p.MaxInstances > 65536 ||
		p.InstancesPerContainer < 1 || p.InstancesPerContainer > 64 || p.BaseInstances < 64 || p.BaseInstances > 4096 ||
		p.HeadroomPercent < 25 || p.HeadroomPercent > 400 ||
		p.MinWatches < 8192 || p.MaxWatches < p.MinWatches || p.MaxWatches > 16777216 ||
		p.WatchesPerContainer < 1 || p.WatchesPerContainer > 65536 ||
		p.WarningPercent < 50 || p.WarningPercent > 95 {
		return p, fmt.Errorf("invalid node resource policy schema or bounds")
	}
	return p, nil
}
