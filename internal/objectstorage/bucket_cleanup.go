package objectstorage

import (
	"context"
	"errors"
	"time"

	"fugue/internal/model"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

type CleanupBatch struct {
	DeletedObjects          int64
	AbortedMultipartUploads int64
	Empty                   bool
}

// EmptyBucketBatch deletes at most eight pages of objects and 16 unfinished
// uploads. It repeatedly reads the first page so key removal cannot make a
// continuation token skip remaining objects. Callers persist progress and
// repeat until Empty. Errors are sanitized to avoid disclosing object keys.
func EmptyBucketBatch(ctx context.Context, backend model.DataBackend) (CleanupBatch, error) {
	var out CleanupBatch
	if backend.Bucket == "" || backend.Endpoint == "" || backend.Credentials.AccessKeyID == "" || backend.Credentials.SecretAccessKey == "" {
		return out, errors.New("incomplete object storage cleanup connection")
	}
	region := backend.Region
	if region == "" {
		region = "auto"
	}
	client := s3.New(s3.Options{
		Region:           region,
		Credentials:      credentials.NewStaticCredentialsProvider(backend.Credentials.AccessKeyID, backend.Credentials.SecretAccessKey, ""),
		UsePathStyle:     true,
		BaseEndpoint:     aws.String(backend.Endpoint),
		RetryMaxAttempts: 1,
	})
	started := time.Now()
	for pageNo := 0; pageNo < 8; pageNo++ {
		list := func() ([]types.ObjectIdentifier, error) {
			bucket := aws.String(backend.Bucket)
			if backend.Provider == model.DataBackendProviderBackblazeB2 {
				page, err := client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: bucket, MaxKeys: aws.Int32(1000)})
				if err != nil {
					return nil, err
				}
				objects := make([]types.ObjectIdentifier, 0, len(page.Versions)+len(page.DeleteMarkers))
				for _, v := range page.Versions {
					objects = append(objects, types.ObjectIdentifier{Key: v.Key, VersionId: v.VersionId})
				}
				for _, v := range page.DeleteMarkers {
					objects = append(objects, types.ObjectIdentifier{Key: v.Key, VersionId: v.VersionId})
				}
				for _, v := range objects {
					if v.Key == nil || aws.ToString(v.VersionId) == "" {
						return nil, errors.New("B2 returned an invalid object version")
					}
				}
				if len(objects) == 0 && aws.ToBool(page.IsTruncated) {
					return nil, errors.New("B2 returned an incomplete versions page")
				}
				return objects, nil
			}
			page, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: bucket, MaxKeys: aws.Int32(1000)})
			if err != nil {
				return nil, err
			}
			objects := make([]types.ObjectIdentifier, 0, len(page.Contents))
			for _, obj := range page.Contents {
				if obj.Key == nil {
					return nil, errors.New("provider returned an invalid object key")
				}
				objects = append(objects, types.ObjectIdentifier{Key: obj.Key})
			}
			return objects, nil
		}
		objects, err := list()
		// Newly created bucket-scoped credentials may not have propagated to
		// S3 yet. Retry only the first read, only for authorization errors, and
		// never retry an ambiguous delete operation here.
		if pageNo == 0 {
			for attempt := 0; err != nil && attempt < 24; attempt++ {
				var apiErr smithy.APIError
				if !errors.As(err, &apiErr) || (apiErr.ErrorCode() != "Unauthorized" && apiErr.ErrorCode() != "AccessDenied" && apiErr.ErrorCode() != "InvalidAccessKeyId") {
					break
				}
				select {
				case <-ctx.Done():
					return out, errors.New("R2 object deletion listing cancelled; retry deletion to resume")
				case <-time.After(500 * time.Millisecond):
				}
				objects, err = list()
			}
			started = time.Now()
		}
		if err != nil {
			return out, errors.New("R2 object deletion listing failed; retry deletion to resume")
		}
		if len(objects) == 0 {
			break
		}
		deleted, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(backend.Bucket), Delete: &types.Delete{Objects: objects, Quiet: aws.Bool(true)}})
		if err != nil {
			return out, errors.New("R2 object deletion failed; retry deletion to resume")
		}
		out.DeletedObjects += int64(len(objects) - len(deleted.Errors))
		if len(deleted.Errors) > 0 {
			return out, errors.New("R2 rejected one or more object deletions; retry deletion to resume")
		}
		if pageNo == 7 || time.Since(started) >= 5*time.Second {
			return out, nil
		}
	}
	page, err := client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: aws.String(backend.Bucket), MaxUploads: aws.Int32(16)})
	if err != nil {
		return out, errors.New("R2 multipart deletion listing failed; retry deletion to resume")
	}
	for i, upload := range page.Uploads {
		if i >= 16 {
			return out, nil
		}
		if upload.Key == nil || upload.UploadId == nil {
			return out, errors.New("R2 returned an invalid multipart upload")
		}
		if _, err = client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: aws.String(backend.Bucket), Key: upload.Key, UploadId: upload.UploadId}); err != nil {
			return out, errors.New("R2 multipart upload abort failed; retry deletion to resume")
		}
		out.AbortedMultipartUploads++
		if time.Since(started) >= 5*time.Second {
			return out, nil
		}
	}
	// The next request verifies emptiness after aborts. The provider's empty-
	// bucket DELETE remains the final guard against concurrent writes.
	out.Empty = len(page.Uploads) == 0 && !aws.ToBool(page.IsTruncated)
	return out, nil
}
