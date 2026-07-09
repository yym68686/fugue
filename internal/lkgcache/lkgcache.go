package lkgcache

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode"
)

const EnvelopeSchemaVersionV1 = "1.0"

type Candidate struct {
	Path string
	Data []byte
}

type Envelope struct {
	SchemaVersion string          `json:"schema_version"`
	Kind          string          `json:"kind"`
	Generation    string          `json:"generation"`
	ContentHash   string          `json:"content_hash"`
	Signature     string          `json:"signature,omitempty"`
	ExpiresAt     time.Time       `json:"expires_at"`
	CreatedAt     time.Time       `json:"created_at"`
	Payload       json.RawMessage `json:"payload"`
}

type ReadEnvelopeOptions struct {
	Now              time.Time
	ExpectedKind     string
	RequireSignature bool
	VerifySignature  func(Envelope) error
}

func NewEnvelope(kind, generation string, payload []byte, expiresAt time.Time, now time.Time) (Envelope, error) {
	kind = strings.TrimSpace(kind)
	generation = strings.TrimSpace(generation)
	if kind == "" {
		return Envelope{}, fmt.Errorf("lkg envelope kind is required")
	}
	if generation == "" {
		return Envelope{}, fmt.Errorf("lkg envelope generation is required")
	}
	if len(bytes.TrimSpace(payload)) == 0 {
		return Envelope{}, fmt.Errorf("lkg envelope payload is required")
	}
	if !json.Valid(payload) {
		return Envelope{}, fmt.Errorf("lkg envelope payload must be valid JSON")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if expiresAt.IsZero() {
		return Envelope{}, fmt.Errorf("lkg envelope expires_at is required")
	}
	return Envelope{
		SchemaVersion: EnvelopeSchemaVersionV1,
		Kind:          kind,
		Generation:    generation,
		ContentHash:   payloadHash(payload),
		ExpiresAt:     expiresAt.UTC(),
		CreatedAt:     now.UTC(),
		Payload:       append(json.RawMessage(nil), payload...),
	}, nil
}

func WriteEnvelope(path string, envelope Envelope, archiveLimit int) error {
	data, err := json.MarshalIndent(envelope, "", "  ")
	if err != nil {
		return err
	}
	return WriteCurrent(path, envelope.Generation, data, archiveLimit)
}

func ReadEnvelope(path string, opts ReadEnvelopeOptions) (Envelope, []byte, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return Envelope{}, nil, fmt.Errorf("lkg envelope path is required")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return Envelope{}, nil, err
	}
	envelope, err := DecodeEnvelope(data, opts)
	if err != nil {
		return Envelope{}, nil, err
	}
	return envelope, append([]byte(nil), envelope.Payload...), nil
}

func DecodeEnvelope(data []byte, opts ReadEnvelopeOptions) (Envelope, error) {
	var envelope Envelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return Envelope{}, fmt.Errorf("decode lkg envelope: %w", err)
	}
	if envelope.SchemaVersion != EnvelopeSchemaVersionV1 {
		return Envelope{}, fmt.Errorf("unsupported lkg envelope schema_version %q", envelope.SchemaVersion)
	}
	if expected := strings.TrimSpace(opts.ExpectedKind); expected != "" && !strings.EqualFold(strings.TrimSpace(envelope.Kind), expected) {
		return Envelope{}, fmt.Errorf("unexpected lkg envelope kind %q, expected %q", envelope.Kind, expected)
	}
	if strings.TrimSpace(envelope.Generation) == "" {
		return Envelope{}, fmt.Errorf("lkg envelope generation is required")
	}
	if len(bytes.TrimSpace(envelope.Payload)) == 0 || !json.Valid(envelope.Payload) {
		return Envelope{}, fmt.Errorf("lkg envelope payload must be valid JSON")
	}
	if got, want := payloadHash(envelope.Payload), strings.TrimSpace(envelope.ContentHash); !strings.EqualFold(got, want) {
		return Envelope{}, fmt.Errorf("lkg envelope content hash mismatch: got %s want %s", got, want)
	}
	now := opts.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if envelope.ExpiresAt.IsZero() {
		return Envelope{}, fmt.Errorf("lkg envelope expires_at is required")
	}
	if !now.Before(envelope.ExpiresAt.UTC()) {
		return Envelope{}, fmt.Errorf("lkg envelope expired at %s", envelope.ExpiresAt.UTC().Format(time.RFC3339))
	}
	if opts.RequireSignature && strings.TrimSpace(envelope.Signature) == "" && opts.VerifySignature == nil {
		return Envelope{}, fmt.Errorf("lkg envelope signature is required")
	}
	if opts.VerifySignature != nil {
		if err := opts.VerifySignature(envelope); err != nil {
			return Envelope{}, fmt.Errorf("verify lkg envelope signature: %w", err)
		}
	}
	return envelope, nil
}

func payloadHash(payload []byte) string {
	var canonical bytes.Buffer
	if err := json.Compact(&canonical, bytes.TrimSpace(payload)); err == nil {
		payload = canonical.Bytes()
	} else {
		payload = bytes.TrimSpace(payload)
	}
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func PreviousPath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	return path + ".previous"
}

func ArchiveDir(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	return path + ".versions"
}

func ArchivePath(path, generation string) string {
	dir := ArchiveDir(path)
	name := sanitizeName(generation)
	if dir == "" || name == "" {
		return ""
	}
	return filepath.Join(dir, name+".json")
}

func WriteCurrent(path, generation string, data []byte, limit int) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	if generationPath := ArchivePath(path, generation); generationPath != "" {
		if err := os.MkdirAll(filepath.Dir(generationPath), 0o755); err != nil {
			return err
		}
		if err := AtomicWriteFile(generationPath, data, 0o600); err != nil {
			return fmt.Errorf("write cache archive: %w", err)
		}
	}
	if err := AtomicWriteFile(path, data, 0o600); err != nil {
		return err
	}
	PruneArchives(path, limit)
	return nil
}

func PreservePrevious(path string, validCurrent func([]byte) bool) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil || len(strings.TrimSpace(string(data))) == 0 {
		return err
	}
	if validCurrent != nil && !validCurrent(data) {
		return nil
	}
	return AtomicWriteFile(PreviousPath(path), data, 0o600)
}

func FallbackCandidates(path string) []Candidate {
	out := make([]Candidate, 0)
	if previous := PreviousPath(path); previous != "" {
		if data, err := os.ReadFile(previous); err == nil && len(strings.TrimSpace(string(data))) > 0 {
			out = append(out, Candidate{Path: previous, Data: data})
		}
	}
	entries := archiveEntries(path)
	for _, entry := range entries {
		data, err := os.ReadFile(entry.path)
		if err != nil || len(strings.TrimSpace(string(data))) == 0 {
			continue
		}
		out = append(out, Candidate{Path: entry.path, Data: data})
	}
	return out
}

func AtomicWriteFile(path string, data []byte, perm os.FileMode) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		_ = os.Remove(tmpName)
	}()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Chmod(perm); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	syncDir(dir)
	return nil
}

func PruneArchives(path string, limit int) {
	if limit <= 0 {
		return
	}
	entries := archiveEntries(path)
	for index, entry := range entries {
		if index < limit {
			continue
		}
		_ = os.Remove(entry.path)
	}
}

type archiveEntry struct {
	path    string
	modTime int64
}

func archiveEntries(path string) []archiveEntry {
	dir := ArchiveDir(path)
	if dir == "" {
		return nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	out := make([]archiveEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		out = append(out, archiveEntry{path: filepath.Join(dir, entry.Name()), modTime: info.ModTime().UnixNano()})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].modTime != out[j].modTime {
			return out[i].modTime > out[j].modTime
		}
		return out[i].path > out[j].path
	})
	return out
}

func syncDir(dir string) {
	handle, err := os.Open(dir)
	if err != nil {
		return
	}
	defer handle.Close()
	_ = handle.Sync()
}

func sanitizeName(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	var builder strings.Builder
	for _, r := range value {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '.' || r == '_' || r == '-' {
			builder.WriteRune(r)
			continue
		}
		builder.WriteByte('_')
	}
	return strings.Trim(builder.String(), "._-")
}
