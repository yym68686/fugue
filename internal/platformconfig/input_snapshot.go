package platformconfig

import (
	"bytes"
	"encoding/json"
)

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
