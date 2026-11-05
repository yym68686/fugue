package inputarchive

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// S3Config is intentionally separate from the backup/data backend config. A
// caller must explicitly authorize a bucket and prefix for compiler-input
// archives; this package never discovers or reuses a backup bucket.
type S3Config struct {
	Endpoint        string
	AccountID       string
	Region          string
	Bucket          string
	Prefix          string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
}

func (c S3Config) Validate() error {
	if strings.TrimSpace(c.Bucket) == "" || strings.TrimSpace(c.AccessKeyID) == "" || strings.TrimSpace(c.SecretAccessKey) == "" || !validPrefix(strings.Trim(c.Prefix, "/")) {
		return ErrInvalid
	}
	if strings.TrimSpace(c.Region) == "" {
		return ErrInvalid
	}
	if account := strings.TrimSpace(c.AccountID); account != "" && !archiveAccountID.MatchString(account) {
		return ErrInvalid
	}
	if endpoint := strings.TrimSpace(c.Endpoint); endpoint != "" {
		u, err := url.Parse(endpoint)
		if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" && u.Path != "/" {
			return ErrInvalid
		}
	}
	return nil
}

var archiveAccountID = regexp.MustCompile(`^[a-f0-9]{32}$`)

// ConfigFromEnv reads only an explicitly separate archive bucket. Absent
// configuration disables the feature; partial configuration is an error, not
// an implicit fallback to the backup or project object store.
func ConfigFromEnv() (S3Config, bool, error) {
	get := func(k string) string { return strings.TrimSpace(os.Getenv(k)) }
	c := S3Config{
		Endpoint: get("FUGUE_COMPILER_INPUT_ARCHIVE_ENDPOINT"), AccountID: get("FUGUE_COMPILER_INPUT_ARCHIVE_ACCOUNT_ID"),
		Region: get("FUGUE_COMPILER_INPUT_ARCHIVE_REGION"), Bucket: get("FUGUE_COMPILER_INPUT_ARCHIVE_BUCKET"), Prefix: get("FUGUE_COMPILER_INPUT_ARCHIVE_PREFIX"),
		AccessKeyID: get("FUGUE_COMPILER_INPUT_ARCHIVE_ACCESS_KEY_ID"), SecretAccessKey: get("FUGUE_COMPILER_INPUT_ARCHIVE_SECRET_ACCESS_KEY"), SessionToken: get("FUGUE_COMPILER_INPUT_ARCHIVE_SESSION_TOKEN"),
	}
	values := []string{c.Endpoint, c.AccountID, c.Region, c.Bucket, c.Prefix, c.AccessKeyID, c.SecretAccessKey, c.SessionToken}
	configured := false
	for _, value := range values {
		if value != "" {
			configured = true
			break
		}
	}
	if !configured {
		return S3Config{}, false, nil
	}
	if c.Region == "" {
		c.Region = "auto"
	}
	if c.Endpoint == "" && c.AccountID != "" {
		c.Endpoint = "https://" + c.AccountID + ".r2.cloudflarestorage.com"
	}
	if err := c.Validate(); err != nil {
		return S3Config{}, true, err
	}
	return c, true, nil
}

// MigrationEnabledFromEnv is a separate opt-in. Merely configuring a bucket
// installs compatible readers and reader leases; it never migrates or changes
// retention behavior by itself.
func MigrationEnabledFromEnv() (bool, error) {
	return explicitFeatureFlag("FUGUE_COMPILER_INPUT_ARCHIVE_MIGRATION_ENABLED")
}

func RoutingEnabledFromEnv() (bool, error) {
	return explicitFeatureFlag("FUGUE_COMPILER_INPUT_ARCHIVE_ROUTING_ENABLED")
}

func explicitFeatureFlag(name string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(name))) {
	case "", "false":
		return false, nil
	case "true":
		return true, nil
	default:
		return false, ErrInvalid
	}
}

func (c S3Config) Fingerprint() (string, error) {
	if err := c.Validate(); err != nil {
		return "", err
	}
	identity := struct{ Endpoint, AccountID, Region, Bucket, Prefix string }{strings.TrimRight(c.Endpoint, "/"), c.AccountID, c.Region, c.Bucket, strings.Trim(c.Prefix, "/")}
	raw, err := json.Marshal(identity)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

type S3Objects struct {
	client         *s3.Client
	bucket, prefix string
}

func NewS3(c S3Config) (*S3Objects, error) {
	return newS3(c, false)
}

// NewSingleAttemptS3 is for explicitly budgeted one-shot copies. The regular
// reader/writer client retains its existing retry behavior.
func NewSingleAttemptS3(c S3Config) (*S3Objects, error) {
	return newS3(c, true)
}

func newS3(c S3Config, singleAttempt bool) (*S3Objects, error) {
	c.Prefix = strings.Trim(c.Prefix, "/")
	if c.Endpoint == "" && c.AccountID != "" {
		c.Endpoint = "https://" + c.AccountID + ".r2.cloudflarestorage.com"
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	opts := s3.Options{Region: c.Region, UsePathStyle: strings.TrimSpace(c.Endpoint) != "", Credentials: credentials.NewStaticCredentialsProvider(c.AccessKeyID, c.SecretAccessKey, c.SessionToken)}
	if singleAttempt {
		opts.RetryMaxAttempts = 1
	}
	if c.Endpoint != "" {
		opts.BaseEndpoint = aws.String(strings.TrimRight(c.Endpoint, "/"))
	}
	return &S3Objects{client: s3.New(opts), bucket: c.Bucket, prefix: c.Prefix}, nil
}

func (s *S3Objects) key(key string) (string, error) {
	if key == "" || pathClean(key) != key {
		return "", ErrInvalid
	}
	if s.prefix == "" {
		return key, nil
	}
	return s.prefix + "/" + key, nil
}

// pathClean is kept local so key validation cannot accidentally normalize a
// caller's path and escape its configured archive prefix.
func pathClean(v string) string {
	if strings.ContainsRune(v, '\x00') || strings.HasPrefix(v, "/") || strings.Contains(v, "\\") {
		return ""
	}
	parts := strings.Split(v, "/")
	for _, p := range parts {
		if p == "" || p == "." || p == ".." {
			return ""
		}
	}
	return v
}

func (s *S3Objects) Put(ctx context.Context, key string, body []byte) error {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if s == nil || s.client == nil {
		return ErrInvalid
	}
	key, err := s.key(key)
	if err != nil {
		return err
	}
	if len(body) == 0 || len(body) > MaxSegmentBytes+storedOverhead {
		return ErrInvalid
	}
	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key), Body: bytes.NewReader(body), ContentLength: aws.Int64(int64(len(body))), IfNoneMatch: aws.String("*")})
	return err
}

func (s *S3Objects) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	if s == nil || s.client == nil {
		cancel()
		return nil, ErrInvalid
	}
	key, err := s.key(key)
	if err != nil {
		cancel()
		return nil, err
	}
	resp, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil {
		cancel()
		return nil, err
	}
	if resp.ContentLength != nil && *resp.ContentLength > MaxSegmentBytes+storedOverhead {
		resp.Body.Close()
		cancel()
		return nil, fmt.Errorf("archive object exceeds bounded size: %w", ErrIntegrity)
	}
	return &archiveObjectBody{ReadCloser: resp.Body, cancel: cancel}, nil
}

type archiveObjectBody struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b *archiveObjectBody) Close() error { defer b.cancel(); return b.ReadCloser.Close() }

func IsAlreadyExists(err error) bool {
	var api smithy.APIError
	if errors.As(err, &api) {
		switch api.ErrorCode() {
		case "PreconditionFailed", "EntityAlreadyExists", "ConditionalRequestConflict":
			return true
		}
	}
	return false
}
