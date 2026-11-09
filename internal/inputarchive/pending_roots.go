package inputarchive

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
)

const PendingRootSchema = "fugue.compiler-input.pending-root.v1"

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
