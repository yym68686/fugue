package platformconfig

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

func canonicalPhysicalEvidence(raw json.RawMessage) (json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil || len(bytes.Trim(raw[decoder.InputOffset():], " \t\r\n")) != 0 {
		return nil, fmt.Errorf("invalid physical evidence JSON")
	}
	return json.Marshal(value)
}

func canonicalizeDNSSelectionEvidence(fact *DNSSelectionObservation) error {
	// Capture can copy the same evidence into both fields. Only exact input
	// equality permits reuse; neither observation metadata nor a claimed digest
	// establishes content equality.
	if len(fact.RetainedPhysicalEvidence) != 0 {
		same := bytes.Equal(fact.PhysicalEvidence, fact.RetainedPhysicalEvidence)
		canonical, err := canonicalPhysicalEvidence(fact.RetainedPhysicalEvidence)
		if err != nil {
			return err
		}
		fact.RetainedPhysicalEvidence = canonical
		if same {
			fact.PhysicalEvidence = bytes.Clone(canonical)
			return nil
		}
	}
	if len(fact.PhysicalEvidence) != 0 {
		canonical, err := canonicalPhysicalEvidence(fact.PhysicalEvidence)
		if err != nil {
			return err
		}
		fact.PhysicalEvidence = canonical
	}
	return nil
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
