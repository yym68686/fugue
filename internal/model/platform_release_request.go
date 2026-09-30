package model

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// An explicit null must never silently remove a caller's requested CAS guard.
func (r *PlatformArtifactReleaseRequest) UnmarshalJSON(raw []byte) error {
	type plain PlatformArtifactReleaseRequest
	var value plain
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&value); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	if guard, exists := fields["producer_reconfiguration"]; exists && bytes.Equal(bytes.TrimSpace(guard), []byte("null")) {
		return fmt.Errorf("producer_reconfiguration cannot be null")
	}
	*r = PlatformArtifactReleaseRequest(value)
	return nil
}
