package routeartifact

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"

	pc "fugue/internal/platformconfig"
)

// A compiler product, never an activation or a health observation. Records
// include the stable wire addresses/TTL; the phases hold group-specific
// routes/certificate owners, DNS requirements and consumer selectors.
type hostedProgramImage struct {
	Schema      string                                 `json:"schema"`
	Host        string                                 `json:"host"`
	DraftDigest string                                 `json:"draft_digest"`
	Base        pc.PreviousTrafficPublicationReference `json:"base"`
	Records     []pc.DNSIntent                         `json:"records"`
	Prepared    HostedPhase                            `json:"prepared"`
	Forward     HostedPhase                            `json:"forward"`
}

const maxHostedProgramBytes = 4 << 20

type CompiledHostedProgram struct {
	Prepared, Forward, Digest string
	Wire                      []byte
}

// FreezeHostedProgram binds every byte (including fields not used by the
// v1 route probes) without changing the existing prepare-offer identities.
// Consumers must authenticate the original draft and their selection before
// using these bytes. In particular the body does not confer a fresh lease.
func FreezeHostedProgram(s pc.HostedAddressSource, p pc.HostedScopeProposal, draftDigest string) (CompiledHostedProgram, error) {
	var zero CompiledHostedProgram
	if len(draftDigest) != 71 || draftDigest[:7] != "sha256:" {
		return zero, errors.New("invalid host draft digest")
	}
	if raw, e := hex.DecodeString(draftDigest[7:]); e != nil || hex.EncodeToString(raw) != draftDigest[7:] {
		return zero, errors.New("invalid host draft digest")
	}
	a, b, e := BuildHostedProgramUnits(s, p)
	if e != nil {
		return zero, e
	}
	x, y, e := hostedProgramPins(p, a, b)
	if e != nil {
		return zero, e
	}
	body, e := json.Marshal(hostedProgramImage{"fugue.hosted-program/v1", p.Host, draftDigest, p.Base, p.Records, a, b})
	if e != nil {
		return zero, e
	}
	if len(body) > maxHostedProgramBytes {
		return zero, errors.New("bounded host program too large")
	}
	digest := sha256.Sum256(body)
	return CompiledHostedProgram{x, y, "sha256:" + hex.EncodeToString(digest[:]), body}, nil
}

// VerifyStoredBytes compares a stored encoding with an independently compiled
// and trusted program. Comparing just the probes or a digest supplied by the
// storage row would allow substitution of unproved bookkeeping/foreign rows.
func (p CompiledHostedProgram) VerifyStoredBytes(digest string, body []byte) bool {
	return len(body) > 0 && len(body) <= maxHostedProgramBytes && p.Digest == digest && bytes.Equal(p.Wire, body)
}
