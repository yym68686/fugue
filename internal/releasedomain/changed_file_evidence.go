package releasedomain

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
	"unicode/utf8"
)

const (
	// ChangedFileEvidenceAPIVersion, ChangedFileEvidenceKind, and
	// ChangedFileEvidencePolicy identify the revision-bound evidence emitted by
	// cmd/fugue-release-domain-evidence.
	ChangedFileEvidenceAPIVersion = "release-domain.fugue.dev/v1"
	ChangedFileEvidenceKind       = "ChangedFileEvidence"
	ChangedFileEvidencePolicy     = "refs-only-offline-v1"

	maxChangedFileEvidenceBytes       = 32 << 20
	maxChangedFileEvidenceStringBytes = 8 << 20
	maxChangedFileEvidenceChanges     = 100_000
)

// changedFileEvidencePayload is the canonical digest payload shared with the
// evidence producer. Digest is deliberately excluded.
type changedFileEvidencePayload struct {
	APIVersion   string        `json:"apiVersion"`
	Kind         string        `json:"kind"`
	Policy       string        `json:"policy"`
	BaseCommit   string        `json:"baseCommit"`
	TargetCommit string        `json:"targetCommit"`
	Changes      []ChangedFile `json:"changes"`
}

type changedFileEvidenceDocument struct {
	APIVersion   string        `json:"apiVersion"`
	Kind         string        `json:"kind"`
	Policy       string        `json:"policy"`
	BaseCommit   string        `json:"baseCommit"`
	TargetCommit string        `json:"targetCommit"`
	Changes      []ChangedFile `json:"changes"`
	Digest       string        `json:"digest"`
}

// ChangedFileEvidence is an immutable, externally revision-bound view of one
// verified evidence document. Its changes are exposed only through a deep
// copy so a caller cannot mutate a later classification through this value.
type ChangedFileEvidence struct {
	baseCommit   string
	targetCommit string
	changes      []ChangedFile
	digest       string
}

// BaseCommit returns the exact trusted 40-hex base commit.
func (evidence ChangedFileEvidence) BaseCommit() string { return evidence.baseCommit }

// TargetCommit returns the exact trusted 40-hex target commit.
func (evidence ChangedFileEvidence) TargetCommit() string { return evidence.targetCommit }

// Digest returns the recomputed canonical payload digest.
func (evidence ChangedFileEvidence) Digest() string { return evidence.digest }

// Changes returns a deep copy of the enriched changed-file evidence.
func (evidence ChangedFileEvidence) Changes() []ChangedFile {
	return cloneChangedFiles(evidence.changes)
}

// DecodeAndVerifyChangedFileEvidence strictly decodes the evidence producer's
// current schema, recomputes its canonical payload digest, and binds its refs
// to two independently trusted full Git commit IDs. The trusted refs must be
// exact lowercase 40-hex SHA-1 object IDs; revision expressions and abbreviated
// IDs are never accepted at this boundary.
func DecodeAndVerifyChangedFileEvidence(reader io.Reader, trustedBaseCommit, trustedTargetCommit string) (ChangedFileEvidence, error) {
	if err := validateTrustedGitCommit(trustedBaseCommit, "trusted base commit"); err != nil {
		return ChangedFileEvidence{}, err
	}
	if err := validateTrustedGitCommit(trustedTargetCommit, "trusted target commit"); err != nil {
		return ChangedFileEvidence{}, err
	}
	if isNilReader(reader) {
		return ChangedFileEvidence{}, fmt.Errorf("changed-file evidence reader is nil")
	}

	data, err := io.ReadAll(io.LimitReader(reader, maxChangedFileEvidenceBytes+1))
	if err != nil {
		return ChangedFileEvidence{}, fmt.Errorf("read changed-file evidence: %w", err)
	}
	if len(data) > maxChangedFileEvidenceBytes {
		return ChangedFileEvidence{}, fmt.Errorf("changed-file evidence exceeds %d-byte limit", maxChangedFileEvidenceBytes)
	}
	if !utf8.Valid(data) {
		return ChangedFileEvidence{}, fmt.Errorf("decode changed-file evidence: input contains invalid UTF-8")
	}
	if err := validateJSONUnicodeEscapes(data); err != nil {
		return ChangedFileEvidence{}, fmt.Errorf("decode changed-file evidence: %w", err)
	}
	if err := validateStrictChangedFileEvidenceJSON(data); err != nil {
		return ChangedFileEvidence{}, fmt.Errorf("decode changed-file evidence: %w", err)
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var document changedFileEvidenceDocument
	if err := decoder.Decode(&document); err != nil {
		return ChangedFileEvidence{}, fmt.Errorf("decode changed-file evidence: %w", err)
	}
	if document.APIVersion != ChangedFileEvidenceAPIVersion {
		return ChangedFileEvidence{}, fmt.Errorf("changed-file evidence apiVersion %q is unsupported", document.APIVersion)
	}
	if document.Kind != ChangedFileEvidenceKind {
		return ChangedFileEvidence{}, fmt.Errorf("changed-file evidence kind %q is unsupported", document.Kind)
	}
	if document.Policy != ChangedFileEvidencePolicy {
		return ChangedFileEvidence{}, fmt.Errorf("changed-file evidence policy %q is unsupported", document.Policy)
	}
	if err := validateTrustedGitCommit(document.BaseCommit, "evidence base commit"); err != nil {
		return ChangedFileEvidence{}, err
	}
	if err := validateTrustedGitCommit(document.TargetCommit, "evidence target commit"); err != nil {
		return ChangedFileEvidence{}, err
	}
	if subtle.ConstantTimeCompare([]byte(document.BaseCommit), []byte(trustedBaseCommit)) != 1 {
		return ChangedFileEvidence{}, fmt.Errorf("trusted base commit mismatch")
	}
	if subtle.ConstantTimeCompare([]byte(document.TargetCommit), []byte(trustedTargetCommit)) != 1 {
		return ChangedFileEvidence{}, fmt.Errorf("trusted target commit mismatch")
	}
	if err := validateChangedFileEvidenceBudget(document); err != nil {
		return ChangedFileEvidence{}, err
	}
	for index, change := range document.Changes {
		if _, err := parseChangeStatus(string(change.Status)); err != nil {
			return ChangedFileEvidence{}, fmt.Errorf("changed-file evidence change %d: %w", index, err)
		}
	}
	if err := validateCanonicalSHA256Digest(document.Digest, "changed-file evidence digest"); err != nil {
		return ChangedFileEvidence{}, err
	}

	payload := changedFileEvidencePayload{
		APIVersion:   document.APIVersion,
		Kind:         document.Kind,
		Policy:       document.Policy,
		BaseCommit:   document.BaseCommit,
		TargetCommit: document.TargetCommit,
		Changes:      document.Changes,
	}
	encodedPayload, err := json.Marshal(payload)
	if err != nil {
		return ChangedFileEvidence{}, fmt.Errorf("encode canonical changed-file evidence payload: %w", err)
	}
	digest := digestBytesSHA256(encodedPayload)
	if subtle.ConstantTimeCompare([]byte(document.Digest), []byte(digest)) != 1 {
		return ChangedFileEvidence{}, fmt.Errorf("changed-file evidence canonical payload digest mismatch")
	}

	return ChangedFileEvidence{
		baseCommit:   document.BaseCommit,
		targetCommit: document.TargetCommit,
		changes:      cloneChangedFiles(document.Changes),
		digest:       digest,
	}, nil
}

func validateStrictChangedFileEvidenceJSON(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := validateJSONValueForType(decoder, reflect.TypeOf(changedFileEvidenceDocument{}), "changedFileEvidence"); err != nil {
		return err
	}
	if trailing, err := decoder.Token(); err != io.EOF {
		if err != nil {
			return fmt.Errorf("decode trailing data: %w", err)
		}
		return fmt.Errorf("changed-file evidence must contain one JSON value; found trailing token %v", trailing)
	}
	return nil
}

func validateChangedFileEvidenceBudget(document changedFileEvidenceDocument) error {
	if len(document.Changes) > maxChangedFileEvidenceChanges {
		return fmt.Errorf("changed-file evidence change count %d exceeds limit %d", len(document.Changes), maxChangedFileEvidenceChanges)
	}
	remaining := maxChangedFileEvidenceStringBytes
	consume := func(value string) error {
		if len(value) > remaining {
			return fmt.Errorf("changed-file evidence string bytes exceed limit %d", maxChangedFileEvidenceStringBytes)
		}
		remaining -= len(value)
		return nil
	}
	for _, value := range []string{document.BaseCommit, document.TargetCommit} {
		if err := consume(value); err != nil {
			return err
		}
	}
	for _, change := range document.Changes {
		if err := consume(change.Path); err != nil {
			return err
		}
		for _, value := range change.ValuePointers {
			if err := consume(value); err != nil {
				return err
			}
		}
		for _, domain := range change.ConsumerDomains {
			if err := consume(string(domain)); err != nil {
				return err
			}
		}
		for _, domain := range change.SemanticDomains {
			if err := consume(string(domain)); err != nil {
				return err
			}
		}
		for _, value := range change.OutsideConsumers {
			if err := consume(value); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateTrustedGitCommit(value, label string) error {
	if !utf8.ValidString(value) || len(value) != 40 {
		return fmt.Errorf("%s must be exact lowercase 40-hex", label)
	}
	for _, digit := range value {
		if (digit < '0' || digit > '9') && (digit < 'a' || digit > 'f') {
			return fmt.Errorf("%s must be exact lowercase 40-hex", label)
		}
	}
	return nil
}

func validateCanonicalSHA256Digest(value, label string) error {
	if !utf8.ValidString(value) || len(value) != len("sha256:")+sha256.Size*2 || !strings.HasPrefix(value, "sha256:") {
		return fmt.Errorf("%s must be lowercase sha256:<64-hex>", label)
	}
	for _, digit := range value[len("sha256:"):] {
		if (digit < '0' || digit > '9') && (digit < 'a' || digit > 'f') {
			return fmt.Errorf("%s must be lowercase sha256:<64-hex>", label)
		}
	}
	return nil
}

func digestBytesSHA256(data []byte) string {
	digest := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func cloneChangedFiles(changes []ChangedFile) []ChangedFile {
	result := make([]ChangedFile, len(changes))
	for index, change := range changes {
		result[index] = change
		result[index].ValuePointers = append([]string(nil), change.ValuePointers...)
		result[index].ConsumerDomains = append([]Domain(nil), change.ConsumerDomains...)
		result[index].SemanticDomains = append([]Domain(nil), change.SemanticDomains...)
		result[index].OutsideConsumers = append([]string(nil), change.OutsideConsumers...)
	}
	return result
}
