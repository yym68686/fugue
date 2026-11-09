package edgecontrol

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"time"
)

const groupCompileInputAttestationSchema = "fugue.group-compile-input.attestation.v1"

// GroupCompileInputAttestation seals the full native input, including fields
// intentionally omitted from semantic compilation hashes. It is a prerequisite
// for source authentication, not proof of custody or a complete live registry.
type GroupCompileInputAttestation struct {
	Schema          string    `json:"schema"`
	GroupID         string    `json:"edge_group_id"`
	Sequence        uint64    `json:"candidate_sequence"`
	CapturedAt      time.Time `json:"captured_at"`
	InputDigest     string    `json:"input_digest"`
	KeyID           string    `json:"key_id"`
	KeyringRevision uint64    `json:"keyring_revision"`
	Signature       string    `json:"signature"`
}

func groupCompileInputSourceDigest(input GroupCompileInput) string {
	input.Attestation = nil
	return digestJSON(input)
}

func validGroupCompileInputAttestation(input GroupCompileInput) bool {
	a := input.Attestation
	if a == nil || a.Schema != groupCompileInputAttestationSchema || a.GroupID != input.Entry.GroupID ||
		a.Sequence != input.Entry.Sequence || !a.CapturedAt.Equal(input.Entry.RecordedAt) || a.CapturedAt.IsZero() ||
		a.KeyringRevision == 0 || !groupAuthorityKeyIDPattern.MatchString(a.KeyID) || a.InputDigest != groupCompileInputSourceDigest(input) {
		return false
	}
	digest, err := hex.DecodeString(strings.TrimPrefix(a.InputDigest, "sha256:"))
	signature, sigErr := base64.RawURLEncoding.DecodeString(a.Signature)
	return strings.HasPrefix(a.InputDigest, "sha256:") && err == nil && len(digest) == sha256.Size && sigErr == nil && len(signature) == sha256.Size && base64.RawURLEncoding.EncodeToString(signature) == a.Signature
}

func groupCompileInputMAC(a GroupCompileInputAttestation, key []byte) []byte {
	a.Signature = ""
	raw, _ := json.Marshal(a)
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(groupCompileInputAttestationSchema + "\x00"))
	_, _ = mac.Write(raw)
	return mac.Sum(nil)
}

// Called only after native witness/CAS/replay validation under the writer lock.
// There is no public operation to attest a stored historical source.
func sealGroupCompileInput(ctx context.Context, input *GroupCompileInput, keyringDir string) error {
	if ctx.Err() != nil || input == nil || input.Attestation != nil || keyringDir == "" {
		return ErrGroupCompileInput
	}
	path := filepath.Join(keyringDir, input.Entry.GroupID+".json")
	keys, err := loadGroupBundleSigningKeyring(path, input.Entry.GroupID)
	if err != nil {
		return ErrGroupCompileInput
	}
	primary, previous, ok := groupOriginalVerificationKeys(keys)
	defer zeroBytes(primary)
	defer zeroBytes(previous)
	if !ok {
		return ErrGroupCompileInput
	}
	a := GroupCompileInputAttestation{Schema: groupCompileInputAttestationSchema, GroupID: input.Entry.GroupID,
		Sequence: input.Entry.Sequence, CapturedAt: input.Entry.RecordedAt, InputDigest: groupCompileInputSourceDigest(*input),
		KeyID: keys.PrimaryKeyID, KeyringRevision: keys.generation}
	a.Signature = base64.RawURLEncoding.EncodeToString(groupCompileInputMAC(a, primary))
	after, err := loadGroupBundleSigningKeyring(path, input.Entry.GroupID)
	if err != nil || ctx.Err() != nil || !reflect.DeepEqual(keys, after) {
		return ErrGroupCompileInput
	}
	input.Attestation = &a
	return nil
}

func verifyGroupCompileInputAttestation(input GroupCompileInput, keys groupBundleSigningKey) bool {
	if !validGroupCompileInputAttestation(input) || keys.GroupID != input.Entry.GroupID || keys.generation == 0 || input.Attestation.KeyringRevision > keys.generation {
		return false
	}
	a := *input.Attestation
	for _, revoked := range keys.RevokedKeyIDs {
		if revoked == a.KeyID {
			return false
		}
	}
	primary, previous, ok := groupOriginalVerificationKeys(keys)
	defer zeroBytes(primary)
	defer zeroBytes(previous)
	if !ok {
		return false
	}
	var key []byte
	switch a.KeyID {
	case keys.PrimaryKeyID:
		key = primary
	case keys.PreviousKeyID:
		key = previous
	default:
		return false
	}
	if len(key) == 0 {
		return false
	}
	signature, _ := base64.RawURLEncoding.DecodeString(a.Signature)
	return hmac.Equal(signature, groupCompileInputMAC(a, key))
}
