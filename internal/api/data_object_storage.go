package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithy "github.com/aws/smithy-go"
)

const (
	defaultDataPresignTTL       = 15 * time.Minute
	minDataPresignTTL           = 5 * time.Minute
	maxDataPresignTTL           = 24 * time.Hour
	dataMultipartPartSize int64 = 64 * 1024 * 1024
)

type dataObjectBackend struct {
	backend model.DataBackend
	client  *s3.Client
	presign *s3.PresignClient
}

type dataObjectInfo struct {
	Key          string
	Size         int64
	LastModified time.Time
}

func newDataObjectBackend(backend model.DataBackend) (*dataObjectBackend, error) {
	backend.Provider = model.NormalizeDataBackendProvider(backend.Provider)
	if !backend.Capabilities.S3Compatible && backend.Provider != model.DataBackendProviderCloudflareR2 {
		return nil, fmt.Errorf("data backend %s is not S3-compatible", backend.Name)
	}
	if strings.TrimSpace(backend.Bucket) == "" {
		return nil, fmt.Errorf("data backend %s has no bucket configured", backend.Name)
	}
	if strings.TrimSpace(backend.Credentials.AccessKeyID) == "" || strings.TrimSpace(backend.Credentials.SecretAccessKey) == "" {
		return nil, fmt.Errorf("data backend %s has no access key credentials configured", backend.Name)
	}
	region := strings.TrimSpace(backend.Region)
	if region == "" {
		region = "auto"
	}
	opts := s3.Options{
		Region:       region,
		Credentials:  credentials.NewStaticCredentialsProvider(backend.Credentials.AccessKeyID, backend.Credentials.SecretAccessKey, backend.Credentials.Token),
		UsePathStyle: strings.TrimSpace(backend.Endpoint) != "",
	}
	if endpoint := strings.TrimRight(strings.TrimSpace(backend.Endpoint), "/"); endpoint != "" {
		opts.BaseEndpoint = aws.String(endpoint)
	}
	client := s3.New(opts)
	return &dataObjectBackend{backend: backend, client: client, presign: s3.NewPresignClient(client)}, nil
}

func dataPresignTTL() time.Duration {
	raw := strings.TrimSpace(os.Getenv("FUGUE_DATA_PRESIGN_TTL"))
	if raw == "" {
		return defaultDataPresignTTL
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return defaultDataPresignTTL
	}
	if value < minDataPresignTTL {
		return minDataPresignTTL
	}
	if value > maxDataPresignTTL {
		return maxDataPresignTTL
	}
	return value
}

func dataBackendSupportsDirectObjectStorage(backend model.DataBackend) bool {
	if strings.TrimSpace(backend.Bucket) == "" {
		return false
	}
	switch model.NormalizeDataBackendProvider(backend.Provider) {
	case model.DataBackendProviderCloudflareR2, model.DataBackendProviderBackblazeB2, model.DataBackendProviderS3, model.DataBackendProviderMinIO:
		return true
	default:
		return backend.Capabilities.S3Compatible
	}
}

func (b *dataObjectBackend) objectKey(objectKey string) string {
	objectKey = strings.Trim(strings.TrimSpace(objectKey), "/")
	prefix := strings.Trim(strings.TrimSpace(b.backend.Prefix), "/")
	if prefix == "" {
		return objectKey
	}
	return path.Join(prefix, objectKey)
}

func (b *dataObjectBackend) headObject(ctx context.Context, objectKey string) (bool, error) {
	_, exists, err := b.headObjectInfo(ctx, objectKey)
	return exists, err
}

func (b *dataObjectBackend) headObjectInfo(ctx context.Context, objectKey string) (dataObjectInfo, bool, error) {
	resp, err := b.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b.backend.Bucket), Key: aws.String(b.objectKey(objectKey))})
	if err == nil {
		return dataObjectInfo{Key: b.objectKey(objectKey), Size: aws.ToInt64(resp.ContentLength), LastModified: aws.ToTime(resp.LastModified)}, true, nil
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NotFound", "NoSuchKey", "404":
			return dataObjectInfo{}, false, nil
		}
	}
	return dataObjectInfo{}, false, err
}

func (b *dataObjectBackend) presignPut(ctx context.Context, objectKey string, ttl time.Duration) (string, time.Time, error) {
	expires := time.Now().UTC().Add(ttl)
	req, err := b.presign.PresignPutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b.backend.Bucket), Key: aws.String(b.objectKey(objectKey))}, func(options *s3.PresignOptions) {
		options.Expires = ttl
	})
	if err != nil {
		return "", time.Time{}, err
	}
	return req.URL, expires, nil
}

func (b *dataObjectBackend) presignGet(ctx context.Context, objectKey string, ttl time.Duration) (string, time.Time, error) {
	expires := time.Now().UTC().Add(ttl)
	req, err := b.presign.PresignGetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b.backend.Bucket), Key: aws.String(b.objectKey(objectKey))}, func(options *s3.PresignOptions) {
		options.Expires = ttl
	})
	if err != nil {
		return "", time.Time{}, err
	}
	return req.URL, expires, nil
}

func (b *dataObjectBackend) getObject(ctx context.Context, objectKey string) (io.ReadCloser, int64, error) {
	resp, err := b.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b.backend.Bucket), Key: aws.String(b.objectKey(objectKey))})
	if err != nil {
		return nil, 0, err
	}
	return resp.Body, aws.ToInt64(resp.ContentLength), nil
}

func (b *dataObjectBackend) putObject(ctx context.Context, objectKey string, body io.Reader, size int64) error {
	url, _, err := b.presignPut(ctx, objectKey, dataPresignTTL())
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, body)
	if err != nil {
		return err
	}
	if size >= 0 {
		req.ContentLength = size
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		message, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("put object %s returned %s: %s", objectKey, resp.Status, strings.TrimSpace(string(message)))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func (b *dataObjectBackend) createMultipartUpload(ctx context.Context, objectKey string) (string, error) {
	out, err := b.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(b.backend.Bucket), Key: aws.String(b.objectKey(objectKey))})
	if err != nil {
		return "", err
	}
	return aws.ToString(out.UploadId), nil
}

func (b *dataObjectBackend) presignUploadPart(ctx context.Context, objectKey, uploadID string, partNumber int32, ttl time.Duration) (string, time.Time, error) {
	expires := time.Now().UTC().Add(ttl)
	req, err := b.presign.PresignUploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(b.backend.Bucket),
		Key:        aws.String(b.objectKey(objectKey)),
		UploadId:   aws.String(uploadID),
		PartNumber: aws.Int32(partNumber),
	}, func(options *s3.PresignOptions) {
		options.Expires = ttl
	})
	if err != nil {
		return "", time.Time{}, err
	}
	return req.URL, expires, nil
}

func (b *dataObjectBackend) listMultipartParts(ctx context.Context, objectKey, uploadID string) ([]model.DataTransferPart, error) {
	var marker *string
	var out []model.DataTransferPart
	for {
		resp, err := b.client.ListParts(ctx, &s3.ListPartsInput{
			Bucket:           aws.String(b.backend.Bucket),
			Key:              aws.String(b.objectKey(objectKey)),
			UploadId:         aws.String(uploadID),
			PartNumberMarker: marker,
		})
		if err != nil {
			return nil, err
		}
		for _, part := range resp.Parts {
			out = append(out, model.DataTransferPart{
				PartNumber: aws.ToInt32(part.PartNumber),
				Size:       aws.ToInt64(part.Size),
				ETag:       strings.Trim(aws.ToString(part.ETag), "\""),
				Completed:  true,
			})
		}
		if !aws.ToBool(resp.IsTruncated) {
			break
		}
		marker = resp.NextPartNumberMarker
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PartNumber < out[j].PartNumber })
	return out, nil
}

func (b *dataObjectBackend) completeMultipartUpload(ctx context.Context, objectKey, uploadID string, parts []model.DataTransferPart) error {
	completed := make([]types.CompletedPart, 0, len(parts))
	for _, part := range parts {
		if part.PartNumber <= 0 || strings.TrimSpace(part.ETag) == "" {
			return fmt.Errorf("part %d is missing etag", part.PartNumber)
		}
		etag := strings.Trim(part.ETag, "\"")
		completed = append(completed, types.CompletedPart{PartNumber: aws.Int32(part.PartNumber), ETag: aws.String(etag)})
	}
	sort.Slice(completed, func(i, j int) bool {
		return aws.ToInt32(completed[i].PartNumber) < aws.ToInt32(completed[j].PartNumber)
	})
	_, err := b.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(b.backend.Bucket),
		Key:             aws.String(b.objectKey(objectKey)),
		UploadId:        aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: completed},
	})
	return err
}

func (b *dataObjectBackend) abortMultipartUpload(ctx context.Context, objectKey, uploadID string) error {
	_, err := b.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(b.backend.Bucket),
		Key:      aws.String(b.objectKey(objectKey)),
		UploadId: aws.String(uploadID),
	})
	return err
}

func (b *dataObjectBackend) listObjects(ctx context.Context, prefix string) ([]dataObjectInfo, error) {
	prefix = b.objectKey(prefix)
	var token *string
	var out []dataObjectInfo
	for {
		resp, err := b.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(b.backend.Bucket), Prefix: aws.String(prefix), ContinuationToken: token})
		if err != nil {
			return nil, err
		}
		for _, obj := range resp.Contents {
			out = append(out, dataObjectInfo{Key: aws.ToString(obj.Key), Size: aws.ToInt64(obj.Size), LastModified: aws.ToTime(obj.LastModified)})
		}
		if !aws.ToBool(resp.IsTruncated) {
			break
		}
		token = resp.NextContinuationToken
	}
	return out, nil
}

func (b *dataObjectBackend) deleteObjects(ctx context.Context, keys []string) error {
	for len(keys) > 0 {
		batch := keys
		if len(batch) > 1000 {
			batch = keys[:1000]
		}
		objects := make([]types.ObjectIdentifier, 0, len(batch))
		for _, key := range batch {
			objects = append(objects, types.ObjectIdentifier{Key: aws.String(key)})
		}
		_, err := b.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(b.backend.Bucket),
			Delete: &types.Delete{Objects: objects, Quiet: aws.Bool(true)},
		})
		if err != nil {
			return err
		}
		keys = keys[len(batch):]
	}
	return nil
}

func directObjectHTTPMethod(blob model.DataTransferPlanBlob) string {
	if blob.UploadMode == model.DataBlobUploadModeMultipart {
		return http.MethodPut
	}
	if blob.UploadURL != "" {
		return http.MethodPut
	}
	return http.MethodGet
}
