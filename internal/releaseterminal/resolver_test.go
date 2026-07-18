package releaseterminal

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

func TestResolveCarrierSnapshotAcceptsCanonicalRootAndLinearCarriers(t *testing.T) {
	genesis := reservation(AbsentOID, "201", testHeadSHA)
	genesisSnapshot := carrierSnapshot(t, testReservationOID, nil, genesis)
	resolved, err := ResolveCarrierSnapshot(genesisSnapshot)
	if err != nil {
		t.Fatalf("ResolveCarrierSnapshot(genesis) error = %v", err)
	}
	if resolved != genesis {
		t.Fatalf("ResolveCarrierSnapshot(genesis) = %#v, want %#v", resolved, genesis)
	}

	success := finalization(ModeSuccess, "success", testReservationOID, "201", testHeadSHA)
	successSnapshot := carrierSnapshot(t, testPreviousOID, []string{testReservationOID}, success)
	resolved, err = ResolveCarrierSnapshot(successSnapshot)
	if err != nil {
		t.Fatalf("ResolveCarrierSnapshot(success) error = %v", err)
	}
	if resolved != success {
		t.Fatalf("ResolveCarrierSnapshot(success) = %#v, want %#v", resolved, success)
	}

	nextReservation := reservation(testPreviousOID, "202", strings.Repeat("4", 40))
	nextSnapshot := carrierSnapshot(t, strings.Repeat("5", 40), []string{testPreviousOID}, nextReservation)
	if _, err := ResolveCarrierSnapshot(nextSnapshot); err != nil {
		t.Fatalf("ResolveCarrierSnapshot(next reservation) error = %v", err)
	}
}

func TestResolveCarrierSnapshotRejectsMalformedCarrierFacts(t *testing.T) {
	genesisDocument := reservation(AbsentOID, "201", testHeadSHA)
	genesis := carrierSnapshot(t, testReservationOID, nil, genesisDocument)
	linearDocument := finalization(ModeFrozen, "failure", testReservationOID, "201", testHeadSHA)
	linear := carrierSnapshot(t, testPreviousOID, []string{testReservationOID}, linearDocument)
	alternativePayload, err := Encode(reservation(AbsentOID, "202", testHeadSHA))
	if err != nil {
		t.Fatalf("Encode(alternative payload) error = %v", err)
	}

	tests := map[string]CarrierSnapshot{
		"object ID":  mutateSnapshot(genesis, func(value *CarrierSnapshot) { value.ObjectOID = strings.Repeat("A", 40) }),
		"empty tree": mutateSnapshot(genesis, func(value *CarrierSnapshot) { value.Entries = nil }),
		"extra tree entry": mutateSnapshot(genesis, func(value *CarrierSnapshot) {
			value.Entries = append(value.Entries, CarrierEntry{Path: "adjacent", Mode: "100644", Type: "blob", OID: testPreviousOID})
		}),
		"payload path":   mutateSnapshot(genesis, func(value *CarrierSnapshot) { value.Entries[0].Path = "state.json" }),
		"payload mode":   mutateSnapshot(genesis, func(value *CarrierSnapshot) { value.Entries[0].Mode = "100755" }),
		"payload type":   mutateSnapshot(genesis, func(value *CarrierSnapshot) { value.Entries[0].Type = "tree" }),
		"blob ID syntax": mutateSnapshot(genesis, func(value *CarrierSnapshot) { value.Entries[0].OID = AbsentOID }),
		"blob content":   mutateSnapshot(genesis, func(value *CarrierSnapshot) { value.Payload = alternativePayload }),
		"noncanonical payload": mutateSnapshot(genesis, func(value *CarrierSnapshot) {
			value.Payload = bytes.TrimSuffix(value.Payload, []byte{'\n'})
			value.Entries[0].OID = gitBlobOID(value.Payload)
		}),
		"genesis parent":        mutateSnapshot(genesis, func(value *CarrierSnapshot) { value.ParentOIDs = []string{testPreviousOID} }),
		"linear missing parent": mutateSnapshot(linear, func(value *CarrierSnapshot) { value.ParentOIDs = nil }),
		"linear multiple parents": mutateSnapshot(linear, func(value *CarrierSnapshot) {
			value.ParentOIDs = []string{testReservationOID, testPreviousOID}
		}),
		"linear wrong parent":  mutateSnapshot(linear, func(value *CarrierSnapshot) { value.ParentOIDs[0] = testPreviousOID }),
		"linear parent syntax": mutateSnapshot(linear, func(value *CarrierSnapshot) { value.ParentOIDs[0] = strings.Repeat("A", 40) }),
	}
	for name, snapshot := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := ResolveCarrierSnapshot(snapshot); err == nil {
				t.Fatal("ResolveCarrierSnapshot() unexpectedly accepted invalid carrier")
			}
		})
	}
}

func TestResolveCarrierSnapshotDoesNotMutateInput(t *testing.T) {
	document := finalization(ModeSuccess, "success", testReservationOID, "201", testHeadSHA)
	snapshot := carrierSnapshot(t, testPreviousOID, []string{testReservationOID}, document)
	wantParents := append([]string(nil), snapshot.ParentOIDs...)
	wantEntries := append([]CarrierEntry(nil), snapshot.Entries...)
	wantPayload := append([]byte(nil), snapshot.Payload...)

	if _, err := ResolveCarrierSnapshot(snapshot); err != nil {
		t.Fatalf("ResolveCarrierSnapshot() error = %v", err)
	}
	if !reflect.DeepEqual(snapshot.ParentOIDs, wantParents) ||
		!reflect.DeepEqual(snapshot.Entries, wantEntries) ||
		!bytes.Equal(snapshot.Payload, wantPayload) {
		t.Fatal("ResolveCarrierSnapshot() mutated caller-owned input")
	}
}

func TestGitBlobOIDMatchesGitObjectFormat(t *testing.T) {
	const want = "d670460b4b4aece5915caf5c68d12f560a9fe3e4"
	if got := gitBlobOID([]byte("test content\n")); got != want {
		t.Fatalf("gitBlobOID() = %q, want %q", got, want)
	}
}

func carrierSnapshot(t *testing.T, objectOID string, parents []string, document Document) CarrierSnapshot {
	t.Helper()
	payload, err := Encode(document)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	return CarrierSnapshot{
		ObjectOID:  objectOID,
		ParentOIDs: append([]string(nil), parents...),
		Entries: []CarrierEntry{{
			Path: CarrierPayloadPath,
			Mode: "100644",
			Type: "blob",
			OID:  gitBlobOID(payload),
		}},
		Payload: append([]byte(nil), payload...),
	}
}

func mutateSnapshot(snapshot CarrierSnapshot, apply func(*CarrierSnapshot)) CarrierSnapshot {
	snapshot.ParentOIDs = append([]string(nil), snapshot.ParentOIDs...)
	snapshot.Entries = append([]CarrierEntry(nil), snapshot.Entries...)
	snapshot.Payload = append([]byte(nil), snapshot.Payload...)
	apply(&snapshot)
	return snapshot
}
