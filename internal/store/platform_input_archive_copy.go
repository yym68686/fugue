package store

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"fugue/internal/inputarchive"
)

// CompilerInputArchiveCopyContract is a single exact input, not a selector or
// a route/retirement authorization. All payload budgets are explicit.
type CompilerInputArchiveCopyContract struct {
	Version        int                                     `json:"version"`
	Digest         string                                  `json:"digest"`
	SourceFormat   string                                  `json:"source_format"`
	RawBytes       int64                                   `json:"raw_bytes"`
	BackendHash    string                                  `json:"backend_hash"`
	SegmentBytes   int                                     `json:"segment_bytes"`
	MaxPutBytes    int64                                   `json:"max_put_bytes"`
	MaxGetBytes    int64                                   `json:"max_get_bytes"`
	MaxPutRequests int64                                   `json:"max_put_requests"`
	MaxGetRequests int64                                   `json:"max_get_requests"`
	BytesPerSecond int64                                   `json:"payload_bytes_per_second"`
	MaxSeconds     int                                     `json:"max_seconds"`
	Resume         *CompilerInputArchiveCopyResumeContract `json:"resume,omitempty"`
}

// CompilerInputArchiveCopyResumeContract binds a new attempt to a previously
// reviewed immutable receipt. The trusted runner must independently verify
// that Kubernetes receipt's UID, content hash and prior declaration identity;
// the isolated worker has no Kubernetes token and cannot do that itself.
type CompilerInputArchiveCopyResumeContract struct {
	SourceManifestSHA256 string `json:"source_manifest_sha256"`
	ReceiptsSHA256       string `json:"receipts_sha256"`
	Prefix               string `json:"prefix"`
	Epoch                int64  `json:"epoch"`
	Token                int64  `json:"token"`
	VerifiedSegments     int    `json:"verified_segments"`
	VerifiedStoredBytes  int64  `json:"verified_stored_bytes"`
	PriorAttemptSHA256   string `json:"prior_attempt_sha256"`
	PriorReceiptUID      string `json:"prior_receipt_uid"`
	PriorReceiptSHA256   string `json:"prior_receipt_sha256"`
}

func (c CompilerInputArchiveCopyContract) resumeExpectation() *inputArchiveResumeExpectation {
	if c.Resume == nil {
		return nil
	}
	r := c.Resume
	return &inputArchiveResumeExpectation{SourceManifestSHA256: r.SourceManifestSHA256, ReceiptsSHA256: r.ReceiptsSHA256,
		BackendHash: c.BackendHash, Prefix: r.Prefix, Epoch: r.Epoch, Token: r.Token, Segments: r.VerifiedSegments, StoredBytes: r.VerifiedStoredBytes}
}

func (c CompilerInputArchiveCopyContract) Validate() error {
	if (c.Version != 1 && c.Version != 2) || !inputDigestPattern.MatchString(c.Digest) || !inputDigestPattern.MatchString(c.BackendHash) ||
		(c.SourceFormat != inputFormatV1 && c.SourceFormat != inputFormatCodecV2) || c.RawBytes < 1 || c.RawBytes > inputarchive.MaxInputBytes ||
		c.SegmentBytes < 64<<10 || c.SegmentBytes > 1<<20 || c.MaxPutBytes < 1 || c.MaxPutBytes > 2<<30 ||
		c.MaxGetBytes < 1 || c.MaxGetBytes > 4<<30 || c.MaxPutRequests < 1 || c.MaxPutRequests > 16384 ||
		c.MaxGetRequests < 1 || c.MaxGetRequests > 32768 || c.BytesPerSecond < 64<<10 || c.BytesPerSecond > 8<<20 || c.MaxSeconds < 60 || c.MaxSeconds > 600 {
		return ErrInvalidInput
	}
	segments := (c.RawBytes + int64(c.SegmentBytes) - 1) / int64(c.SegmentBytes)
	priorSegments := int64(0)
	if c.Version == 1 {
		if c.Resume != nil {
			return ErrInvalidInput
		}
	} else {
		if c.Resume == nil || c.resumeExpectation().validate() != nil ||
			!inputDigestPattern.MatchString(c.Resume.PriorAttemptSHA256) || !inputDigestPattern.MatchString(c.Resume.PriorReceiptSHA256) ||
			!archiveResumeReceiptUIDPattern.MatchString(c.Resume.PriorReceiptUID) ||
			c.Resume.VerifiedSegments >= int(segments) {
			return ErrInvalidInput
		}
		if (inputarchive.Manifest{Format: inputarchive.Format, Digest: c.Digest, RawBytes: c.RawBytes, SegmentBytes: c.SegmentBytes, Prefix: c.Resume.Prefix}).ValidatePlan() != nil {
			return ErrInvalidInput
		}
		priorSegments = int64(c.Resume.VerifiedSegments)
		if c.Resume.VerifiedStoredBytes > priorSegments*(int64(c.SegmentBytes)+(1<<20)) {
			return ErrInvalidInput
		}
	}
	if c.MaxPutRequests != segments-priorSegments || c.MaxGetRequests != 2*segments {
		return ErrInvalidInput
	}
	return nil
}

type CompilerInputArchiveCopyReport struct {
	Action           string                 `json:"action"`
	Digest           string                 `json:"digest"`
	SourceFormat     string                 `json:"source_format"`
	RawBytes         int64                  `json:"raw_bytes"`
	VerifiedRawBytes int64                  `json:"verified_raw_bytes"`
	Segments         int                    `json:"segments"`
	State            string                 `json:"state"`
	SourcePreserved  bool                   `json:"source_preserved"`
	RouteUnchanged   bool                   `json:"route_unchanged"`
	Usage            inputarchive.CopyUsage `json:"usage"`
	ElapsedMS        int64                  `json:"elapsed_ms"`
}

// RunCompilerInputArchiveCopy deliberately requires explicit role sessions;
// platform encryption/signing keys must never be passed to the isolated Job.
// It has no routing, hot-copy, pin, fence, GC, or retirement call path.
func RunCompilerInputArchiveCopy(ctx context.Context, databaseURL string, c CompilerInputArchiveCopyContract) (report CompilerInputArchiveCopyReport, err error) {
	report = CompilerInputArchiveCopyReport{Action: "archive-copy", Digest: c.Digest, SourceFormat: c.SourceFormat, RawBytes: c.RawBytes}
	if err = c.Validate(); err != nil {
		return
	}
	enabled, flagErr := inputarchive.MigrationEnabledFromEnv()
	routing, routeErr := inputarchive.RoutingEnabledFromEnv()
	if flagErr != nil || !enabled || routeErr != nil || routing {
		err = ErrInvalidInput
		return
	}
	for _, key := range []string{"FUGUE_DATA_CREDENTIAL_ENCRYPTION_KEY", "FUGUE_BUNDLE_SIGNING_KEY", "FUGUE_WORKLOAD_IDENTITY_SIGNING_KEY"} {
		if os.Getenv(key) != "" {
			err = ErrInvalidInput
			return
		}
	}
	writerConfig, configured, configErr := inputarchive.ConfigFromEnv()
	if configErr != nil || !configured || writerConfig.SessionToken != "" {
		err = ErrInvalidInput
		return
	}
	fingerprint, hashErr := writerConfig.Fingerprint()
	if hashErr != nil || fingerprint != c.BackendHash {
		err = ErrConflict
		return
	}
	readerConfig := writerConfig
	readerConfig.AccessKeyID = os.Getenv("FUGUE_COMPILER_INPUT_ARCHIVE_READER_ACCESS_KEY_ID")
	readerConfig.SecretAccessKey = os.Getenv("FUGUE_COMPILER_INPUT_ARCHIVE_READER_SECRET_ACCESS_KEY")
	if readerConfig.Validate() != nil || readerConfig.AccessKeyID == writerConfig.AccessKeyID {
		err = ErrInvalidInput
		return
	}
	writer, e := inputarchive.NewSingleAttemptS3(writerConfig)
	if e != nil {
		err = e
		return
	}
	reader, e := inputarchive.NewSingleAttemptS3(readerConfig)
	if e != nil {
		err = e
		return
	}
	objects, e := inputarchive.NewBudgetObjects(reader, writer, inputarchive.CopyBudget{MaxPutBytes: c.MaxPutBytes, MaxGetBytes: c.MaxGetBytes, MaxPutRequests: c.MaxPutRequests, MaxGetRequests: c.MaxGetRequests, BytesPerSecond: c.BytesPerSecond})
	if e != nil {
		err = e
		return
	}
	start := time.Now()
	defer func() { report.Usage = objects.Usage(); report.ElapsedMS = time.Since(start).Milliseconds() }()
	ctx, cancel := context.WithTimeout(ctx, time.Duration(c.MaxSeconds)*time.Second)
	defer cancel()
	db, e := openPostgresDatabase(databaseURL)
	if e != nil {
		err = e
		return
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	r, e := copyCompilerInputArchive(ctx, db, objects, fingerprint, CompilerInputArchiveMigrationOptions{Digest: c.Digest, Apply: true, SegmentBytes: c.SegmentBytes,
		expectedFormat: c.SourceFormat, expectedRawBytes: c.RawBytes, requireNew: c.Version == 1, expectedResume: c.resumeExpectation(),
		sourcePageChunks: 1, wrapSource: func(r io.Reader) io.Reader { return objects.Source(ctx, r) }})
	if e != nil {
		err = e
		return
	}
	if r.State != "verified" || r.RawBytes != c.RawBytes {
		err = fmt.Errorf("copy verification receipt differs: %w", ErrConflict)
		return
	}
	report.VerifiedRawBytes, report.Segments, report.State = r.RawBytes, r.Segments, r.State
	// completeCompilerInputArchive checked the original source manifest in the
	// final fenced transaction. Copy never modifies source chunks or read route.
	report.SourcePreserved, report.RouteUnchanged = true, true
	return
}
