package platformconfig

import (
	"bytes"
	"encoding/json"
	"fmt"
)

func canonicalPhysicalEvidence(raw json.RawMessage) (json.RawMessage, error) {
	if !json.Valid(raw) {
		return nil, fmt.Errorf("invalid physical evidence JSON")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

func DecodeRuntimeSnapshotContent(content map[string]any) (RuntimeSnapshot, error) {
	var snapshot RuntimeSnapshot
	raw, err := json.Marshal(content)
	if err != nil {
		return snapshot, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	err = decoder.Decode(&snapshot)
	return snapshot, err
}

func RuntimeSnapshotContentDigest(content map[string]any) (string, error) {
	snapshot, err := DecodeRuntimeSnapshotContent(content)
	if err != nil {
		return "", err
	}
	return RuntimeSnapshotDigest(snapshot)
}
