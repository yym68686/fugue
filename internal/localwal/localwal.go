package localwal

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

const SchemaVersionV1 = "1.0"

type Record struct {
	SchemaVersion string            `json:"schema_version"`
	ID            string            `json:"id"`
	Component     string            `json:"component"`
	NodeID        string            `json:"node_id,omitempty"`
	Action        string            `json:"action"`
	SafetyClass   string            `json:"safety_class,omitempty"`
	Generation    string            `json:"generation,omitempty"`
	Subject       string            `json:"subject,omitempty"`
	Evidence      map[string]string `json:"evidence,omitempty"`
	EvidenceHash  string            `json:"evidence_hash,omitempty"`
	Signer        string            `json:"signer,omitempty"`
	Signature     string            `json:"signature,omitempty"`
	ExpiresAt     *time.Time        `json:"expires_at,omitempty"`
	RecordedAt    time.Time         `json:"recorded_at"`
}

func NewRecord(component, nodeID, action string, evidence map[string]string, generation string, expiresAt *time.Time, now time.Time) (Record, error) {
	component = strings.TrimSpace(component)
	action = strings.TrimSpace(action)
	if component == "" {
		return Record{}, fmt.Errorf("wal component is required")
	}
	if action == "" {
		return Record{}, fmt.Errorf("wal action is required")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	copiedEvidence := cloneEvidence(evidence)
	return Record{
		SchemaVersion: SchemaVersionV1,
		ID:            model.NewID("wal"),
		Component:     component,
		NodeID:        strings.TrimSpace(nodeID),
		Action:        action,
		Generation:    strings.TrimSpace(generation),
		Evidence:      copiedEvidence,
		EvidenceHash:  EvidenceHash(copiedEvidence),
		ExpiresAt:     normalizeTimePtr(expiresAt),
		RecordedAt:    now.UTC(),
	}, nil
}

func Append(path string, record Record) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return fmt.Errorf("wal path is required")
	}
	if record.SchemaVersion == "" {
		record.SchemaVersion = SchemaVersionV1
	}
	if strings.TrimSpace(record.ID) == "" {
		record.ID = model.NewID("wal")
	}
	if record.RecordedAt.IsZero() {
		record.RecordedAt = time.Now().UTC()
	}
	record.Evidence = cloneEvidence(record.Evidence)
	record.EvidenceHash = EvidenceHash(record.Evidence)
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	handle, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer handle.Close()
	if _, err := handle.Write(append(data, '\n')); err != nil {
		return err
	}
	if err := handle.Sync(); err != nil {
		return err
	}
	syncDir(filepath.Dir(path))
	return nil
}

func ReadAll(path string) ([]Record, error) {
	data, err := os.ReadFile(strings.TrimSpace(path))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	lines := bytes.Split(data, []byte{'\n'})
	records := make([]Record, 0, len(lines))
	for index, line := range lines {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		var record Record
		if err := json.Unmarshal(line, &record); err != nil {
			return nil, fmt.Errorf("decode wal line %d: %w", index+1, err)
		}
		if record.SchemaVersion != SchemaVersionV1 {
			return nil, fmt.Errorf("unsupported wal schema_version %q on line %d", record.SchemaVersion, index+1)
		}
		if got := EvidenceHash(record.Evidence); record.EvidenceHash != "" && !strings.EqualFold(got, record.EvidenceHash) {
			return nil, fmt.Errorf("wal evidence hash mismatch on line %d: got %s want %s", index+1, got, record.EvidenceHash)
		}
		records = append(records, record)
	}
	sort.SliceStable(records, func(i, j int) bool {
		return records[i].RecordedAt.Before(records[j].RecordedAt)
	})
	return records, nil
}

func SignRecord(record Record, signer string, secret []byte) (Record, error) {
	if len(secret) == 0 {
		return Record{}, fmt.Errorf("wal signing secret is required")
	}
	record.Signer = strings.TrimSpace(signer)
	if record.Signer == "" {
		return Record{}, fmt.Errorf("wal signer is required")
	}
	record.Signature = ""
	record.Evidence = cloneEvidence(record.Evidence)
	record.EvidenceHash = EvidenceHash(record.Evidence)
	payload, err := canonicalRecord(record)
	if err != nil {
		return Record{}, err
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(payload)
	record.Signature = "hmac-sha256:" + hex.EncodeToString(mac.Sum(nil))
	return record, nil
}

func VerifyRecord(record Record, secret []byte) error {
	if len(secret) == 0 {
		return fmt.Errorf("wal verification secret is required")
	}
	if strings.TrimSpace(record.Signer) == "" {
		return fmt.Errorf("wal signer is required")
	}
	signature := strings.TrimSpace(record.Signature)
	if signature == "" {
		return fmt.Errorf("wal signature is required")
	}
	unsigned := record
	unsigned.Signature = ""
	if unsigned.EvidenceHash == "" {
		unsigned.EvidenceHash = EvidenceHash(unsigned.Evidence)
	}
	payload, err := canonicalRecord(unsigned)
	if err != nil {
		return err
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(payload)
	want := "hmac-sha256:" + hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(signature), []byte(want)) {
		return fmt.Errorf("wal signature mismatch")
	}
	return nil
}

func canonicalRecord(record Record) ([]byte, error) {
	record.Evidence = cloneEvidence(record.Evidence)
	if record.EvidenceHash == "" {
		record.EvidenceHash = EvidenceHash(record.Evidence)
	}
	return json.Marshal(struct {
		SchemaVersion string            `json:"schema_version"`
		ID            string            `json:"id"`
		Component     string            `json:"component"`
		NodeID        string            `json:"node_id,omitempty"`
		Action        string            `json:"action"`
		SafetyClass   string            `json:"safety_class,omitempty"`
		Generation    string            `json:"generation,omitempty"`
		Subject       string            `json:"subject,omitempty"`
		Evidence      map[string]string `json:"evidence,omitempty"`
		EvidenceHash  string            `json:"evidence_hash,omitempty"`
		Signer        string            `json:"signer,omitempty"`
		ExpiresAt     *time.Time        `json:"expires_at,omitempty"`
		RecordedAt    time.Time         `json:"recorded_at"`
	}{
		SchemaVersion: record.SchemaVersion,
		ID:            record.ID,
		Component:     record.Component,
		NodeID:        record.NodeID,
		Action:        record.Action,
		SafetyClass:   record.SafetyClass,
		Generation:    record.Generation,
		Subject:       record.Subject,
		Evidence:      record.Evidence,
		EvidenceHash:  record.EvidenceHash,
		Signer:        record.Signer,
		ExpiresAt:     normalizeTimePtr(record.ExpiresAt),
		RecordedAt:    record.RecordedAt.UTC(),
	})
}

func EvidenceHash(evidence map[string]string) string {
	if len(evidence) == 0 {
		return "sha256:" + hex.EncodeToString(sha256.New().Sum(nil))
	}
	keys := make([]string, 0, len(evidence))
	for key := range evidence {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, key := range keys {
		b.WriteString(key)
		b.WriteByte('=')
		b.WriteString(evidence[key])
		b.WriteByte('\n')
	}
	sum := sha256.Sum256([]byte(b.String()))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func cloneEvidence(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		out[key] = strings.TrimSpace(value)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func normalizeTimePtr(value *time.Time) *time.Time {
	if value == nil || value.IsZero() {
		return nil
	}
	normalized := value.UTC()
	return &normalized
}

func syncDir(dir string) {
	handle, err := os.Open(dir)
	if err != nil {
		return
	}
	defer handle.Close()
	_ = handle.Sync()
}
