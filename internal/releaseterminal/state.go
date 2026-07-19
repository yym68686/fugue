// Package releaseterminal defines the dormant, canonical release-policy
// terminal-state contract. It does not read or write Git refs and is not
// imported by the runtime release path.
package releaseterminal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"unicode/utf8"
)

const (
	SchemaVersion = 1

	CertificateKindReservation  = "fugue-control-plane-release-policy-terminal-reservation"
	CertificateKindFinalization = "fugue-control-plane-release-policy-terminal-finalization"

	AbsentOID = "absent"

	maxDocumentBytes = 16 << 10
)

// Mode is one immutable state in the release-policy terminal chain.
type Mode string

const (
	ModeReservation Mode = "reservation"
	ModeSuccess     Mode = "success"
	ModeFrozen      Mode = "frozen"
)

// FreezeReason is an administrative reason for freezing a reservation when
// the immutable source run itself completed successfully.
type FreezeReason string

const (
	FreezeReasonReservationStale FreezeReason = "reservation_stale"
)

// Workflow is an allowed source of a release-policy terminal state.
type Workflow string

const (
	WorkflowDeployV2                            Workflow = ".github/workflows/deploy-control-plane-v2.yml"
	WorkflowPromotion                           Workflow = ".github/workflows/promote-control-plane-release-policy.yml"
	WorkflowTerminalWriter                      Workflow = ".github/workflows/write-control-plane-release-terminal-rp1.yml"
	WorkflowTerminalNextReservationMaterializer Workflow = ".github/workflows/materialize-control-plane-release-terminal-next-reservation-rp1.yml"
)

// Document is the complete canonical terminal-state payload. ReservationOID
// is absent on reservations and equals PreviousTerminalStateOID on a success
// or frozen finalization.
type Document struct {
	SchemaVersion            uint64       `json:"schema_version"`
	CertificateKind          string       `json:"certificate_kind"`
	TerminalMode             Mode         `json:"terminal_mode"`
	SourceRunID              string       `json:"source_run_id"`
	SourceRunAttempt         uint64       `json:"source_run_attempt"`
	SourceHeadSHA            string       `json:"source_head_sha"`
	SourceWorkflow           Workflow     `json:"source_workflow"`
	SourceConclusion         string       `json:"source_conclusion"`
	PreviousTerminalStateOID string       `json:"previous_terminal_state_oid"`
	ReservationOID           string       `json:"reservation_oid,omitempty"`
	FreezeReason             FreezeReason `json:"freeze_reason,omitempty"`
}

// Encode validates and returns the only accepted byte representation: compact
// UTF-8 JSON in struct-field order followed by exactly one LF.
func Encode(document Document) ([]byte, error) {
	if err := Validate(document); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		return nil, fmt.Errorf("encode terminal state: %w", err)
	}
	encoded = append(encoded, '\n')
	if len(encoded) > maxDocumentBytes {
		return nil, fmt.Errorf("encoded terminal state exceeds %d bytes", maxDocumentBytes)
	}
	return encoded, nil
}

// Decode accepts only the exact bytes produced by Encode. Unknown fields,
// duplicate keys, alternate key order, whitespace variants, and trailing data
// therefore fail closed.
func Decode(data []byte) (Document, error) {
	if len(data) == 0 {
		return Document{}, fmt.Errorf("terminal state is empty")
	}
	if len(data) > maxDocumentBytes {
		return Document{}, fmt.Errorf("terminal state exceeds %d bytes", maxDocumentBytes)
	}
	if !utf8.Valid(data) {
		return Document{}, fmt.Errorf("terminal state is not valid UTF-8")
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var document Document
	if err := decoder.Decode(&document); err != nil {
		return Document{}, fmt.Errorf("decode terminal state: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return Document{}, fmt.Errorf("terminal state contains trailing JSON")
		}
		return Document{}, fmt.Errorf("decode terminal state trailer: %w", err)
	}
	if err := Validate(document); err != nil {
		return Document{}, err
	}
	canonical, err := Encode(document)
	if err != nil {
		return Document{}, err
	}
	if !bytes.Equal(data, canonical) {
		return Document{}, fmt.Errorf("terminal state is not canonical JSON")
	}
	return document, nil
}

// Validate enforces the fixed state schema without consulting mutable
// external state.
func Validate(document Document) error {
	if document.SchemaVersion != SchemaVersion {
		return fmt.Errorf("terminal state schema version is unsupported")
	}
	if !isCanonicalPositiveDecimal(document.SourceRunID) {
		return fmt.Errorf("terminal state source run ID must be a canonical positive decimal")
	}
	if document.SourceRunAttempt != 1 {
		return fmt.Errorf("terminal state source run attempt must be exactly one")
	}
	if !isLowerHexOID(document.SourceHeadSHA) {
		return fmt.Errorf("terminal state source head SHA must be lowercase 40-hex")
	}
	if document.SourceWorkflow != WorkflowDeployV2 &&
		document.SourceWorkflow != WorkflowPromotion &&
		document.SourceWorkflow != WorkflowTerminalWriter &&
		document.SourceWorkflow != WorkflowTerminalNextReservationMaterializer {
		return fmt.Errorf("terminal state source workflow is unsupported")
	}
	if document.PreviousTerminalStateOID != AbsentOID && !isLowerHexOID(document.PreviousTerminalStateOID) {
		return fmt.Errorf("terminal state previous OID must be absent or lowercase 40-hex")
	}

	switch document.TerminalMode {
	case ModeReservation:
		if document.CertificateKind != CertificateKindReservation {
			return fmt.Errorf("terminal reservation certificate kind is invalid")
		}
		if document.SourceConclusion != "in_progress" {
			return fmt.Errorf("terminal reservation source conclusion must be in_progress")
		}
		if document.ReservationOID != "" {
			return fmt.Errorf("terminal reservation must omit reservation OID")
		}
		if document.FreezeReason != "" {
			return fmt.Errorf("terminal reservation must omit freeze reason")
		}
	case ModeSuccess:
		if err := validateFinalization(document); err != nil {
			return err
		}
		if document.SourceConclusion != "success" {
			return fmt.Errorf("terminal success source conclusion must be success")
		}
		if document.FreezeReason != "" {
			return fmt.Errorf("terminal success must omit freeze reason")
		}
	case ModeFrozen:
		if err := validateFinalization(document); err != nil {
			return err
		}
		if document.FreezeReason == "" && !isFrozenConclusion(document.SourceConclusion) {
			return fmt.Errorf("terminal frozen source conclusion is unsupported")
		}
		if document.FreezeReason != "" {
			if document.FreezeReason != FreezeReasonReservationStale {
				return fmt.Errorf("terminal administrative freeze reason is unsupported")
			}
			if document.SourceConclusion != "success" {
				return fmt.Errorf("terminal stale-reservation freeze requires a successful source conclusion")
			}
		}
	default:
		return fmt.Errorf("terminal state mode is unsupported")
	}
	return nil
}

func validateFinalization(document Document) error {
	if document.CertificateKind != CertificateKindFinalization {
		return fmt.Errorf("terminal finalization certificate kind is invalid")
	}
	if !isLowerHexOID(document.PreviousTerminalStateOID) {
		return fmt.Errorf("terminal finalization must follow one reservation OID")
	}
	if !isLowerHexOID(document.ReservationOID) || document.ReservationOID != document.PreviousTerminalStateOID {
		return fmt.Errorf("terminal finalization reservation OID must equal its previous state OID")
	}
	return nil
}

// ValidateTransition validates one append-only state transition. Genesis must
// reserve first. A reservation may finalize once as success or frozen. Success
// may start the next reservation; frozen is a sink until a separately designed
// recovery checkpoint exists.
func ValidateTransition(previous *Document, previousOID string, next Document) error {
	if err := Validate(next); err != nil {
		return err
	}
	if previous == nil {
		if previousOID != AbsentOID || next.TerminalMode != ModeReservation || next.PreviousTerminalStateOID != AbsentOID {
			return fmt.Errorf("terminal state genesis must be one reservation from absent")
		}
		return nil
	}
	if err := Validate(*previous); err != nil {
		return fmt.Errorf("previous terminal state is invalid: %w", err)
	}
	if !isLowerHexOID(previousOID) {
		return fmt.Errorf("previous terminal state object ID must be lowercase 40-hex")
	}
	if next.PreviousTerminalStateOID != previousOID {
		return fmt.Errorf("terminal state transition does not bind the exact previous object")
	}

	switch previous.TerminalMode {
	case ModeReservation:
		if next.TerminalMode != ModeSuccess && next.TerminalMode != ModeFrozen {
			return fmt.Errorf("terminal reservation must finalize as success or frozen")
		}
		if next.ReservationOID != previousOID || !sameSource(*previous, next) {
			return fmt.Errorf("terminal finalization does not bind its reservation and source")
		}
	case ModeSuccess:
		if next.TerminalMode != ModeReservation {
			return fmt.Errorf("terminal success may only advance to the next reservation")
		}
	case ModeFrozen:
		return fmt.Errorf("terminal frozen state cannot advance without a separate recovery contract")
	default:
		return fmt.Errorf("previous terminal state mode is unsupported")
	}
	return nil
}

func sameSource(left, right Document) bool {
	return left.SourceRunID == right.SourceRunID &&
		left.SourceRunAttempt == right.SourceRunAttempt &&
		left.SourceHeadSHA == right.SourceHeadSHA &&
		left.SourceWorkflow == right.SourceWorkflow
}

func isCanonicalPositiveDecimal(value string) bool {
	if value == "" || value[0] == '0' {
		return false
	}
	for index := range value {
		if value[index] < '0' || value[index] > '9' {
			return false
		}
	}
	return true
}

func isLowerHexOID(value string) bool {
	if len(value) != 40 {
		return false
	}
	for index := range value {
		character := value[index]
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

func isFrozenConclusion(value string) bool {
	switch value {
	case "failure", "cancelled", "timed_out", "action_required", "neutral", "skipped", "stale", "startup_failure":
		return true
	default:
		return false
	}
}
