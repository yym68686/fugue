package releaseterminal

import (
	"crypto/sha1"
	"fmt"
)

const CarrierPayloadPath = ".fugue-release-terminal-state.json"

// CarrierEntry is one immutable fact from a Git tree. ResolveCarrierSnapshot
// accepts exactly one regular payload blob and no adjacent files.
type CarrierEntry struct {
	Path string
	Mode string
	Type string
	OID  string
}

// CarrierSnapshot contains facts already read from one immutable Git commit.
// It deliberately has no repository, network, filesystem, or ref handle.
type CarrierSnapshot struct {
	ObjectOID  string
	ParentOIDs []string
	Entries    []CarrierEntry
	Payload    []byte
}

// ResolveCarrierSnapshot fails closed unless one immutable carrier commit
// contains exactly the canonical terminal-state payload and its physical
// parent chain agrees with the payload's previous object binding.
func ResolveCarrierSnapshot(snapshot CarrierSnapshot) (Document, error) {
	if !isLowerHexOID(snapshot.ObjectOID) {
		return Document{}, fmt.Errorf("terminal carrier object ID must be lowercase 40-hex")
	}
	if len(snapshot.Entries) != 1 {
		return Document{}, fmt.Errorf("terminal carrier tree must contain exactly one entry")
	}
	entry := snapshot.Entries[0]
	if entry.Path != CarrierPayloadPath || entry.Mode != "100644" || entry.Type != "blob" {
		return Document{}, fmt.Errorf("terminal carrier entry must be the canonical regular payload blob")
	}
	if !isLowerHexOID(entry.OID) {
		return Document{}, fmt.Errorf("terminal carrier blob ID must be lowercase 40-hex")
	}
	document, err := Decode(snapshot.Payload)
	if err != nil {
		return Document{}, fmt.Errorf("resolve terminal carrier payload: %w", err)
	}
	if entry.OID != gitBlobOID(snapshot.Payload) {
		return Document{}, fmt.Errorf("terminal carrier payload does not match its blob ID")
	}
	if document.TerminalMode == ModeReservation && document.PreviousTerminalStateOID == AbsentOID {
		if len(snapshot.ParentOIDs) != 0 {
			return Document{}, fmt.Errorf("terminal carrier genesis reservation must be a root commit")
		}
		return document, nil
	}
	if len(snapshot.ParentOIDs) != 1 {
		return Document{}, fmt.Errorf("terminal carrier must have exactly one previous-state parent")
	}
	if !isLowerHexOID(snapshot.ParentOIDs[0]) || snapshot.ParentOIDs[0] != document.PreviousTerminalStateOID {
		return Document{}, fmt.Errorf("terminal carrier parent does not match the document previous object")
	}
	return document, nil
}

func gitBlobOID(payload []byte) string {
	// SHA-1 is the object identity algorithm of the repository's 40-hex Git
	// object format; this is not a cryptographic authenticity decision.
	hasher := sha1.New() // #nosec G401 -- required for Git object identity
	_, _ = fmt.Fprintf(hasher, "blob %d%c", len(payload), byte(0))
	_, _ = hasher.Write(payload)
	return fmt.Sprintf("%x", hasher.Sum(nil))
}
