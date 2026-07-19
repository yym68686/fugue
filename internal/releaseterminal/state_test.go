package releaseterminal

import (
	"bytes"
	"strings"
	"testing"
)

const (
	testHeadSHA        = "1111111111111111111111111111111111111111"
	testPreviousOID    = "2222222222222222222222222222222222222222"
	testReservationOID = "3333333333333333333333333333333333333333"
)

func TestEncodeDecodeCanonicalReservation(t *testing.T) {
	document := reservation(AbsentOID, "101", testHeadSHA)
	encoded, err := Encode(document)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	want := "{\"schema_version\":1,\"certificate_kind\":\"fugue-control-plane-release-policy-terminal-reservation\",\"terminal_mode\":\"reservation\",\"source_run_id\":\"101\",\"source_run_attempt\":1,\"source_head_sha\":\"1111111111111111111111111111111111111111\",\"source_workflow\":\".github/workflows/deploy-control-plane-v2.yml\",\"source_conclusion\":\"in_progress\",\"previous_terminal_state_oid\":\"absent\"}\n"
	if string(encoded) != want {
		t.Fatalf("Encode() = %q, want %q", encoded, want)
	}
	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if decoded != document {
		t.Fatalf("Decode() = %#v, want %#v", decoded, document)
	}
	repeated, err := Encode(decoded)
	if err != nil {
		t.Fatalf("second Encode() error = %v", err)
	}
	if !bytes.Equal(encoded, repeated) {
		t.Fatal("canonical terminal-state encoding is not deterministic")
	}
}

func TestEncodeRejectsDocumentThatCannotRoundTripWithinBound(t *testing.T) {
	document := reservation(AbsentOID, strings.Repeat("1", maxDocumentBytes), testHeadSHA)
	if err := Validate(document); err != nil {
		t.Fatalf("Validate() error = %v; fixture must reach the encoded-size boundary", err)
	}
	if encoded, err := Encode(document); err == nil {
		t.Fatalf("Encode() unexpectedly produced %d bytes above the contract bound", len(encoded))
	}
}

func TestDecodeRejectsNonCanonicalAndAmbiguousJSON(t *testing.T) {
	canonical, err := Encode(reservation(AbsentOID, "101", testHeadSHA))
	if err != nil {
		t.Fatal(err)
	}
	tests := map[string][]byte{
		"missing final LF":    canonical[:len(canonical)-1],
		"leading whitespace":  append([]byte(" "), canonical...),
		"trailing whitespace": append(append([]byte(nil), canonical...), ' '),
		"trailing JSON":       append(append([]byte(nil), canonical...), []byte("{}\n")...),
		"unknown field":       []byte(strings.Replace(string(canonical), "}\n", ",\"unknown\":true}\n", 1)),
		"duplicate field":     []byte(strings.Replace(string(canonical), "\"schema_version\":1", "\"schema_version\":1,\"schema_version\":1", 1)),
		"reordered fields":    []byte(strings.Replace(string(canonical), "{\"schema_version\":1,\"certificate_kind\":", "{\"certificate_kind\":", 1)),
		"invalid UTF-8":       {0xff, 0xfe, 0xfd},
		"oversized":           bytes.Repeat([]byte{'x'}, maxDocumentBytes+1),
	}
	for name, payload := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := Decode(payload); err == nil {
				t.Fatal("Decode() unexpectedly accepted invalid bytes")
			}
		})
	}
}

func TestValidateDocumentModes(t *testing.T) {
	validReservation := reservation(AbsentOID, "101", testHeadSHA)
	validSuccess := finalization(ModeSuccess, "success", testReservationOID, "101", testHeadSHA)
	validFrozen := finalization(ModeFrozen, "timed_out", testReservationOID, "101", testHeadSHA)
	for name, document := range map[string]Document{
		"reservation": validReservation,
		"success":     validSuccess,
		"frozen":      validFrozen,
	} {
		t.Run(name, func(t *testing.T) {
			if err := Validate(document); err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
		})
	}

	tests := map[string]Document{
		"schema":                              mutate(validReservation, func(value *Document) { value.SchemaVersion = 2 }),
		"run ID zero":                         mutate(validReservation, func(value *Document) { value.SourceRunID = "0" }),
		"run ID leading zero":                 mutate(validReservation, func(value *Document) { value.SourceRunID = "0101" }),
		"rerun":                               mutate(validReservation, func(value *Document) { value.SourceRunAttempt = 2 }),
		"uppercase SHA":                       mutate(validReservation, func(value *Document) { value.SourceHeadSHA = strings.Repeat("A", 40) }),
		"workflow":                            mutate(validReservation, func(value *Document) { value.SourceWorkflow = ".github/workflows/other.yml" }),
		"reservation kind":                    mutate(validReservation, func(value *Document) { value.CertificateKind = CertificateKindFinalization }),
		"reservation conclusion":              mutate(validReservation, func(value *Document) { value.SourceConclusion = "success" }),
		"reservation self OID":                mutate(validReservation, func(value *Document) { value.ReservationOID = testReservationOID }),
		"success conclusion":                  mutate(validSuccess, func(value *Document) { value.SourceConclusion = "failure" }),
		"frozen success":                      mutate(validFrozen, func(value *Document) { value.SourceConclusion = "success" }),
		"finalization absent parent":          mutate(validSuccess, func(value *Document) { value.PreviousTerminalStateOID = AbsentOID }),
		"finalization mismatched reservation": mutate(validSuccess, func(value *Document) { value.ReservationOID = testPreviousOID }),
		"mode":                                mutate(validReservation, func(value *Document) { value.TerminalMode = "pending" }),
	}
	for name, document := range tests {
		t.Run(name, func(t *testing.T) {
			if err := Validate(document); err == nil {
				t.Fatal("Validate() unexpectedly accepted invalid document")
			}
		})
	}
}

func TestValidateSourceWorkflowIdentities(t *testing.T) {
	for _, workflow := range []Workflow{
		WorkflowDeployV2,
		WorkflowPromotion,
		WorkflowTerminalWriter,
		WorkflowTerminalNextReservationMaterializer,
	} {
		t.Run(string(workflow), func(t *testing.T) {
			document := reservation(AbsentOID, "101", testHeadSHA)
			document.SourceWorkflow = workflow
			if err := Validate(document); err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
		})
	}

	for _, workflow := range []Workflow{
		".github/workflows/write-control-plane-release-terminal-rp1.yml ",
		".github/workflows/WRITE-control-plane-release-terminal-rp1.yml",
		".github/workflows/../workflows/write-control-plane-release-terminal-rp1.yml",
		".github/workflows/write-control-plane-release-terminal-rp1.yaml",
		".github/workflows/materialize-control-plane-release-terminal-next-reservation-rp1.yml ",
		".github/workflows/MATERIALIZE-control-plane-release-terminal-next-reservation-rp1.yml",
		".github/workflows/../workflows/materialize-control-plane-release-terminal-next-reservation-rp1.yml",
		".github/workflows/materialize-control-plane-release-terminal-next-reservation-rp1.yaml",
	} {
		t.Run("reject "+string(workflow), func(t *testing.T) {
			document := reservation(AbsentOID, "101", testHeadSHA)
			document.SourceWorkflow = workflow
			if err := Validate(document); err == nil {
				t.Fatal("Validate() unexpectedly accepted a workflow identity variant")
			}
		})
	}
}

func TestValidateTransitionChain(t *testing.T) {
	firstReservation := reservation(AbsentOID, "101", testHeadSHA)
	if err := ValidateTransition(nil, AbsentOID, firstReservation); err != nil {
		t.Fatalf("genesis reservation: %v", err)
	}

	success := finalization(ModeSuccess, "success", testReservationOID, "101", testHeadSHA)
	if err := ValidateTransition(&firstReservation, testReservationOID, success); err != nil {
		t.Fatalf("reservation to success: %v", err)
	}
	nextReservation := reservation(testPreviousOID, "102", strings.Repeat("4", 40))
	if err := ValidateTransition(&success, testPreviousOID, nextReservation); err != nil {
		t.Fatalf("success to next reservation: %v", err)
	}

	frozen := finalization(ModeFrozen, "failure", testReservationOID, "101", testHeadSHA)
	if err := ValidateTransition(&firstReservation, testReservationOID, frozen); err != nil {
		t.Fatalf("reservation to frozen: %v", err)
	}
	if err := ValidateTransition(&frozen, testPreviousOID, nextReservation); err == nil {
		t.Fatal("frozen terminal state unexpectedly advanced")
	}
}

func TestValidateTransitionRejectsUnboundOrIllegalEdges(t *testing.T) {
	reservationDocument := reservation(AbsentOID, "101", testHeadSHA)
	success := finalization(ModeSuccess, "success", testReservationOID, "101", testHeadSHA)
	tests := map[string]struct {
		previous    *Document
		previousOID string
		next        Document
	}{
		"genesis object present": {nil, testPreviousOID, reservationDocument},
		"genesis finalization":   {nil, AbsentOID, success},
		"reservation repeats":    {&reservationDocument, testReservationOID, reservation(testReservationOID, "102", strings.Repeat("4", 40))},
		"wrong previous OID":     {&reservationDocument, testPreviousOID, success},
		"source run drift":       {&reservationDocument, testReservationOID, mutate(success, func(value *Document) { value.SourceRunID = "102" })},
		"source SHA drift":       {&reservationDocument, testReservationOID, mutate(success, func(value *Document) { value.SourceHeadSHA = strings.Repeat("4", 40) })},
		"terminal repeats": {&success, testPreviousOID, mutate(success, func(value *Document) {
			value.PreviousTerminalStateOID = testPreviousOID
			value.ReservationOID = testPreviousOID
		})},
		"noncanonical object ID": {&success, strings.Repeat("A", 40), reservation(strings.Repeat("A", 40), "102", strings.Repeat("4", 40))},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			if err := ValidateTransition(test.previous, test.previousOID, test.next); err == nil {
				t.Fatal("ValidateTransition() unexpectedly accepted invalid edge")
			}
		})
	}
}

func reservation(previousOID, runID, headSHA string) Document {
	return Document{
		SchemaVersion:            SchemaVersion,
		CertificateKind:          CertificateKindReservation,
		TerminalMode:             ModeReservation,
		SourceRunID:              runID,
		SourceRunAttempt:         1,
		SourceHeadSHA:            headSHA,
		SourceWorkflow:           WorkflowDeployV2,
		SourceConclusion:         "in_progress",
		PreviousTerminalStateOID: previousOID,
	}
}

func finalization(mode Mode, conclusion, reservationOID, runID, headSHA string) Document {
	return Document{
		SchemaVersion:            SchemaVersion,
		CertificateKind:          CertificateKindFinalization,
		TerminalMode:             mode,
		SourceRunID:              runID,
		SourceRunAttempt:         1,
		SourceHeadSHA:            headSHA,
		SourceWorkflow:           WorkflowDeployV2,
		SourceConclusion:         conclusion,
		PreviousTerminalStateOID: reservationOID,
		ReservationOID:           reservationOID,
	}
}

func mutate(document Document, apply func(*Document)) Document {
	apply(&document)
	return document
}
