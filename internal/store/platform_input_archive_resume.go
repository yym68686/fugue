package store

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"regexp"
	"time"

	"fugue/internal/inputarchive"
)

// An exact expectation never grants execution authority. The isolated runner
// must bind a separately authorized attempt before passing a v2 copy contract.
var archiveResumeReceiptUIDPattern = regexp.MustCompile(`^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$`)

type inputArchiveResumeExpectation struct {
	SourceManifestSHA256 string
	ReceiptsSHA256       string
	BackendHash          string
	Prefix               string
	Epoch                int64
	Token                int64
	Segments             int
	StoredBytes          int64
}

func (e inputArchiveResumeExpectation) validate() error {
	if !inputDigestPattern.MatchString(e.SourceManifestSHA256) || !inputDigestPattern.MatchString(e.ReceiptsSHA256) || !inputDigestPattern.MatchString(e.BackendHash) ||
		e.Epoch < 0 || e.Token < 1 || e.Token == math.MaxInt64 || e.Segments < 1 || e.Segments > inputarchive.MaxSegments ||
		e.StoredBytes < 1 || e.StoredBytes > 2*inputarchive.MaxInputBytes || len(e.Prefix) < 1 || len(e.Prefix) > 512 {
		return ErrInvalidInput
	}
	return nil
}

func inputArchiveIdentityHash(value any) string {
	// These internal identities contain only strings, bounded integers and
	// segment structs; encoding cannot fail and never includes source bytes.
	encoded, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	h := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(h[:])
}

func inputArchiveSourceIdentityHash(m inputManifest) string {
	return inputArchiveIdentityHash(struct {
		Digest, Format, Location string
		RawBytes                 int64
		ChunkSize, ChunkCount    int
		CreatedUTC               string
	}{m.Digest, m.Format, m.Location, m.Size, m.ChunkSize, m.Count, m.Created.UTC().Format(time.RFC3339Nano)})
}

func (e inputArchiveResumeExpectation) matches(c inputArchiveClaim) error {
	if e.validate() != nil || c.manifest.ValidatePlan() != nil || c.epoch != e.Epoch || c.token != e.Token ||
		c.manifest.Prefix != e.Prefix || inputArchiveSourceIdentityHash(c.source) != e.SourceManifestSHA256 ||
		len(c.manifest.Segments) != e.Segments || inputArchiveIdentityHash(c.manifest.Segments) != e.ReceiptsSHA256 {
		return ErrConflict
	}
	var stored int64
	for _, segment := range c.manifest.Segments {
		stored += int64(segment.StoredBytes)
	}
	if stored != e.StoredBytes {
		return ErrConflict
	}
	return nil
}
