package platformconfig

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
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
	raw, err := json.Marshal(content)
	if err != nil {
		return RuntimeSnapshot{}, err
	}
	return DecodeRuntimeSnapshotJSON(raw)
}

func DecodeRuntimeSnapshotJSON(raw []byte) (RuntimeSnapshot, error) {
	var snapshot RuntimeSnapshot
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&snapshot); err != nil {
		return snapshot, err
	}
	if decoder.Decode(new(any)) != io.EOF {
		return snapshot, fmt.Errorf("runtime snapshot has trailing JSON")
	}
	return snapshot, nil
}

func RuntimeSnapshotContentDigest(content map[string]any) (string, error) {
	snapshot, err := DecodeRuntimeSnapshotContent(content)
	if err != nil {
		return "", err
	}
	return RuntimeSnapshotDigest(snapshot)
}
