package inputarchive

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
)

const PendingRootSchema = "fugue.compiler-input.pending-root.v1"

const PendingRootRequestSchema = "fugue.compiler-input.pending-root-request.v1"
const PendingRootSourceBytes = 16 << 20

var ErrPendingRoot = errors.New("compiler input pending retention root invalid")
var pendingRootHash = regexp.MustCompile(`^[0-9a-f]{64}$`)

// PendingRootRegistration adds retention only. SourceDigest is an exact source
// identity, NOT a caller assertion of source authentication, original SHA,
// pin verification, serving authority, or completeness of the live registry.
// The authenticated transport must independently bind Owner to its principal.
// Pending roots have no automatic expiry, release, or replacement operation.
type PendingRootRegistration struct {
	Schema       string         `json:"schema"`
	Owner        string         `json:"owner"`
	Generation   string         `json:"generation"`
	SourceDigest string         `json:"source_digest"`
	References   []HotReference `json:"references"`
}

// PendingRootReceipt acknowledges this exact append-only retention record.
// It never authorizes publication/activation or substitutes for verified pins.
type PendingRootReceipt struct {
	RegistrationSHA256 string `json:"registration_sha256"`
	RegistryRevision   int64  `json:"registry_revision"`
	SourceEpoch        int64  `json:"source_epoch"`
}

// PendingRootRequest carries the original authenticated source, not a client
// boolean asserting that a digest was verified. The source-specific verifier
// must derive the exact registration independently. This is not activation.
type PendingRootRequest struct {
	Schema       string                  `json:"schema"`
	Registration PendingRootRegistration `json:"registration"`
	SourceSchema string                  `json:"source_schema"`
	Source       json.RawMessage         `json:"source"`
}

func (r PendingRootRequest) Canonical() ([]byte, string, error) {
	if r.Schema != PendingRootRequestSchema || !pendingRootText(r.SourceSchema, 256) ||
		len(r.Source) == 0 || len(r.Source) > PendingRootSourceBytes || r.Source[0] != '{' || !json.Valid(r.Source) {
		return nil, "", ErrPendingRoot
	}
	if _, _, err := r.Registration.Canonical(); err != nil {
		return nil, "", err
	}
	raw, err := json.Marshal(r)
	if err != nil || len(raw) > PendingRootSourceBytes+(32<<10) {
		return nil, "", ErrPendingRoot
	}
	sum := sha256.Sum256(raw)
	return raw, hex.EncodeToString(sum[:]), nil
}

// These providers are trusted process configuration, never request fields.
// A source witness must be bound to the complete request and current trust.
// Check runs again inside the native transaction immediately before commit;
// implementations must not perform remote I/O while native locks are held.
type PendingRootSourceVerifier interface {
	VerifyPendingRootSource(context.Context, PendingRootRequest) (PendingRootSourceWitness, error)
}

type PendingRootSourceWitness interface {
	CheckPendingRootSource(context.Context, PendingRootRequest) error
}

// Admission must reserve physical allocation, WAL, temporary and mandatory
// failure/exit costs for this exact operation under current policy. A count
// limit or an available-space observation alone is not such a reservation.
// There is deliberately no permissive default implementation.
type PendingRootAdmission interface {
	ReservePendingRoot(context.Context, PendingRootRequest) (PendingRootReservation, error)
}

type PendingRootReservation interface {
	CheckPendingRootReservation(context.Context) error
	RecordPendingRootOutcome(PendingRootOutcome)
}

type PendingRootOutcome string

const (
	PendingRootNotAttempted           PendingRootOutcome = "not-attempted"
	PendingRootCompletionNotConfirmed PendingRootOutcome = "completion-not-confirmed"
	PendingRootCommitted              PendingRootOutcome = "committed"
)

func (r PendingRootRegistration) Canonical() ([]byte, string, error) {
	if r.Schema != PendingRootSchema || !pendingRootText(r.Owner, 256) || !pendingRootText(r.Generation, 512) ||
		!pendingRootHash.MatchString(strings.TrimPrefix(r.SourceDigest, "sha256:")) || !strings.HasPrefix(r.SourceDigest, "sha256:") ||
		len(r.References) == 0 || len(r.References) > 32 {
		return nil, "", ErrPendingRoot
	}
	previous := ""
	for _, ref := range r.References {
		if !pendingRootText(ref.ID, 512) || ref.ArtifactKind != "" || ref.ScopeKey != "" {
			return nil, "", ErrPendingRoot
		}
		switch ref.Kind {
		case "artifact", "input":
			if !digestPattern.MatchString(ref.ExpectedHash) || ref.Kind == "input" && ref.ID != ref.ExpectedHash {
				return nil, "", ErrPendingRoot
			}
		case "release":
			if ref.ExpectedHash != "" {
				return nil, "", ErrPendingRoot
			}
		default:
			return nil, "", ErrPendingRoot
		}
		// An exact identity appears once, in canonical order. A conflicting hash
		// is never deduplicated into an apparently consistent registration.
		key := ref.Kind + "\x00" + ref.ID
		if key <= previous {
			return nil, "", ErrPendingRoot
		}
		previous = key
	}
	raw, err := json.Marshal(r)
	if err != nil || len(raw) > 16<<10 {
		return nil, "", ErrPendingRoot
	}
	sum := sha256.Sum256(raw)
	return raw, hex.EncodeToString(sum[:]), nil
}

func (r PendingRootReceipt) Matches(registration PendingRootRegistration) bool {
	_, hash, err := registration.Canonical()
	return err == nil && r.RegistrationSHA256 == hash && r.RegistryRevision > 0 && r.SourceEpoch >= 0
}

func pendingRootText(s string, limit int) bool {
	return s != "" && len(s) <= limit && s == strings.TrimSpace(s) && !strings.ContainsAny(s, "\x00\r\n\t")
}
