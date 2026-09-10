package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const backupVerifyRangeSize int64 = 64 << 20

// Small objects retain the existing single-GET contract. Large objects are
// verified in bounded reads so a truncated connection does not discard hours
// of export work. The final SHA256 remains mandatory on every path.
func verifyBackupObjectSHA256(ctx context.Context, backend *dataObjectBackend, key string, expectedSize int64, expectedSHA256 string) error {
	if backend == nil {
		return fmt.Errorf("backup object backend is not configured")
	}
	if expectedSize > dataMultipartPartSize {
		return verifyBackupObjectSHA256Ranges(ctx, backend, key, expectedSize, expectedSHA256, backupVerifyRangeSize)
	}
	body, objectSize, err := backend.getObject(ctx, key)
	if err != nil {
		return err
	}
	digest := sha256.New()
	size, readErr := io.Copy(digest, body)
	closeErr := body.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		if ctx.Err() != nil {
			return errors.Join(err, ctx.Err())
		}
		// The server's size can guide recovery when the caller did not know it.
		if expectedSize < 0 {
			expectedSize = objectSize
		}
		if expectedSize > 0 {
			return verifyBackupObjectSHA256Ranges(ctx, backend, key, expectedSize, expectedSHA256, backupVerifyRangeSize)
		}
		return fmt.Errorf("read backup after %d bytes: %w", size, err)
	}
	if expectedSize >= 0 && size != expectedSize {
		return fmt.Errorf("size mismatch: expected %d bytes, got %d bytes", expectedSize, size)
	}
	if objectSize > 0 && expectedSize >= 0 && objectSize != expectedSize {
		return fmt.Errorf("object metadata size mismatch: expected %d bytes, got %d bytes", expectedSize, objectSize)
	}
	return checkBackupDigest(digest.Sum(nil), expectedSHA256)
}

func checkBackupDigest(digest []byte, expected string) error {
	actual := hex.EncodeToString(digest)
	if !strings.EqualFold(strings.TrimSpace(expected), actual) {
		return fmt.Errorf("sha256 mismatch: expected %s, got %s", expected, actual)
	}
	return nil
}

var errBackupRangeMetadata = errors.New("backup range metadata mismatch")

func verifyBackupObjectSHA256Ranges(ctx context.Context, backend *dataObjectBackend, key string, expectedSize int64, expectedSHA256 string, rangeSize int64) error {
	if expectedSize <= 0 || rangeSize <= 0 || rangeSize > backupVerifyRangeSize {
		return fmt.Errorf("invalid backup verification size")
	}
	head, err := backend.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(backend.backend.Bucket), Key: aws.String(backend.objectKey(key)),
	})
	if err != nil {
		return fmt.Errorf("inspect backup for range verification: %w", err)
	}
	if head.ContentLength == nil || *head.ContentLength != expectedSize {
		return fmt.Errorf("object metadata size mismatch: expected %d bytes, got %d bytes", expectedSize, aws.ToInt64(head.ContentLength))
	}
	etag := aws.ToString(head.ETag)
	if etag == "" {
		return fmt.Errorf("backup object ETag missing; cannot bind ranged verification to one object")
	}
	buffer := make([]byte, min(rangeSize, expectedSize))
	digest := sha256.New()
	for offset := int64(0); offset < expectedSize; {
		want := min(rangeSize, expectedSize-offset)
		var readErr error
		var readBytes int
		for attempt := 0; attempt < 3; attempt++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			if attempt > 0 {
				timer := time.NewTimer(time.Duration(attempt) * 200 * time.Millisecond)
				select {
				case <-ctx.Done():
					timer.Stop()
					return ctx.Err()
				case <-timer.C:
				}
			}
			rangeCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
			readBytes, readErr = readBackupRange(rangeCtx, backend, key, etag, offset, expectedSize, buffer[:want])
			cancel()
			if readErr == nil {
				break
			}
			if errors.Is(readErr, errBackupRangeMetadata) {
				return readErr
			}
		}
		if readErr != nil {
			return fmt.Errorf("verify range %d-%d of %d after 3 attempts (read %d bytes): %w", offset, offset+want-1, expectedSize, readBytes, errors.Join(readErr, ctx.Err()))
		}
		// An incomplete attempt never contributes bytes to the digest.
		_, _ = digest.Write(buffer[:want])
		offset += want
	}
	return checkBackupDigest(digest.Sum(nil), expectedSHA256)
}

func readBackupRange(ctx context.Context, backend *dataObjectBackend, key, etag string, offset, total int64, buffer []byte) (int, error) {
	end := offset + int64(len(buffer)) - 1
	resp, err := backend.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(backend.backend.Bucket), Key: aws.String(backend.objectKey(key)),
		Range: aws.String(fmt.Sprintf("bytes=%d-%d", offset, end)), IfMatch: aws.String(etag),
	})
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	wantRange := fmt.Sprintf("bytes %d-%d/%d", offset, end, total)
	if aws.ToString(resp.ContentRange) != wantRange || resp.ContentLength == nil || *resp.ContentLength != int64(len(buffer)) || aws.ToString(resp.ETag) != etag {
		return 0, fmt.Errorf("%w for bytes %d-%d", errBackupRangeMetadata, offset, end)
	}
	n, err := io.ReadFull(resp.Body, buffer)
	if err != nil {
		return n, errors.Join(err, ctx.Err())
	}
	var extra [1]byte
	more, err := io.ReadFull(resp.Body, extra[:])
	if more != 0 || !errors.Is(err, io.EOF) {
		return n, fmt.Errorf("range response has extra bytes or did not end: %w", errors.Join(err, errBackupRangeMetadata))
	}
	return n, nil
}
