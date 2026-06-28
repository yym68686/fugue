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
	"sync"
	"time"

	"fugue/internal/model"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithy "github.com/aws/smithy-go"
)

var errDataBackendConfiguration = errors.New("data backend configuration error")

const (
	defaultDataPresignTTL             = 15 * time.Minute
	minDataPresignTTL                 = 5 * time.Minute
	maxDataPresignTTL                 = 24 * time.Hour
	dataMultipartPartSize       int64 = 64 * 1024 * 1024
	dataMultipartMinPartSize    int64 = 5 * 1024 * 1024
	dataMultipartMaxPartSize    int64 = 5 * 1024 * 1024 * 1024
	dataMultipartMaxParts       int64 = 10000
	dataMultipartAbortTimeout         = 30 * time.Second
	dataObjectDeleteConcurrency       = 8
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

func dataBackendUsesManagedBlobAPI(backend model.DataBackend) bool {
	provider := model.NormalizeDataBackendProvider(backend.Provider)
	return provider == model.DataBackendProviderFugueManaged || backend.Capabilities.FugueManagedBlobAPI
}

func dataObjectBackendForTransfer(backend model.DataBackend) (*dataObjectBackend, error) {
	if dataBackendSupportsDirectObjectStorage(backend) {
		return newDataObjectBackend(backend)
	}
	if dataBackendUsesManagedBlobAPI(backend) {
		return nil, nil
	}
	if _, err := newDataObjectBackend(backend); err != nil {
		return nil, fmt.Errorf("%w: %v", errDataBackendConfiguration, err)
	}
	return nil, fmt.Errorf("%w: data backend %s is not configured for direct object storage", errDataBackendConfiguration, backend.Name)
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
	if size > dataMultipartPartSize {
		partSize, err := dataMultipartPartSizeForObject(size)
		if err != nil {
			return err
		}
		return b.putObjectMultipart(ctx, objectKey, body, size, partSize)
	}
	return b.putObjectSingle(ctx, objectKey, body, size)
}

func (b *dataObjectBackend) putObjectSingle(ctx context.Context, objectKey string, body io.Reader, size int64) error {
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

func (b *dataObjectBackend) putObjectMultipart(ctx context.Context, objectKey string, body io.Reader, size int64, partSize int64) (err error) {
	if size < 0 {
		return fmt.Errorf("multipart upload for %s requires a known object size", objectKey)
	}
	if size == 0 {
		return b.putObjectSingle(ctx, objectKey, body, size)
	}
	if partSize < dataMultipartMinPartSize {
		partSize = dataMultipartMinPartSize
	}
	if partSize > dataMultipartMaxPartSize {
		return fmt.Errorf("multipart part size %d exceeds maximum %d", partSize, dataMultipartMaxPartSize)
	}
	if ((size-1)/partSize)+1 > dataMultipartMaxParts {
		if dynamicPartSize, partSizeErr := dataMultipartPartSizeForObject(size); partSizeErr != nil {
			return partSizeErr
		} else {
			partSize = dynamicPartSize
		}
	}

	uploadID, err := b.createMultipartUpload(ctx, objectKey)
	if err != nil {
		return err
	}
	completed := false
	defer func() {
		if completed {
			return
		}
		abortParent := context.Background()
		if ctx != nil {
			abortParent = context.WithoutCancel(ctx)
		}
		abortCtx, cancel := context.WithTimeout(abortParent, dataMultipartAbortTimeout)
		defer cancel()
		if abortErr := b.abortMultipartUpload(abortCtx, objectKey, uploadID); abortErr != nil {
			err = errors.Join(err, fmt.Errorf("abort multipart upload %s: %w", objectKey, abortErr))
		}
	}()

	parts := make([]model.DataTransferPart, 0, int(((size-1)/partSize)+1))
	remaining := size
	for partNumber := int32(1); remaining > 0; partNumber++ {
		if int64(partNumber) > dataMultipartMaxParts {
			return fmt.Errorf("multipart upload for %s requires more than %d parts", objectKey, dataMultipartMaxParts)
		}
		currentPartSize := partSize
		if remaining < currentPartSize {
			currentPartSize = remaining
		}
		partReader := &countingReader{r: io.LimitReader(body, currentPartSize)}
		etag, uploadErr := b.putMultipartPart(ctx, objectKey, uploadID, partNumber, partReader, currentPartSize)
		if uploadErr != nil {
			return fmt.Errorf("upload multipart part %d for %s: %w", partNumber, objectKey, uploadErr)
		}
		if partReader.n != currentPartSize {
			return fmt.Errorf("upload multipart part %d for %s read %d bytes, expected %d", partNumber, objectKey, partReader.n, currentPartSize)
		}
		parts = append(parts, model.DataTransferPart{
			PartNumber: partNumber,
			Size:       currentPartSize,
			ETag:       etag,
			Completed:  true,
		})
		remaining -= currentPartSize
	}
	if err := b.completeMultipartUpload(ctx, objectKey, uploadID, parts); err != nil {
		return fmt.Errorf("complete multipart upload for %s: %w", objectKey, err)
	}
	completed = true
	return nil
}

func (b *dataObjectBackend) putMultipartPart(ctx context.Context, objectKey, uploadID string, partNumber int32, body io.Reader, size int64) (string, error) {
	url, _, err := b.presignUploadPart(ctx, objectKey, uploadID, partNumber, dataPresignTTL())
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, body)
	if err != nil {
		return "", err
	}
	req.ContentLength = size
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		message, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return "", fmt.Errorf("put multipart part %d for object %s returned %s: %s", partNumber, objectKey, resp.Status, strings.TrimSpace(string(message)))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	etag := strings.Trim(resp.Header.Get("ETag"), "\"")
	if etag == "" {
		return "", fmt.Errorf("put multipart part %d for object %s returned empty etag", partNumber, objectKey)
	}
	return etag, nil
}

func dataMultipartPartSizeForObject(size int64) (int64, error) {
	partSize := dataMultipartPartSize
	if size <= 0 {
		return partSize, nil
	}
	minPartSize := ((size - 1) / dataMultipartMaxParts) + 1
	if minPartSize > partSize {
		partSize = roundUpToMultiple(minPartSize, dataMultipartMinPartSize)
	}
	if partSize > dataMultipartMaxPartSize {
		return 0, fmt.Errorf("object size %d exceeds multipart upload limit", size)
	}
	return partSize, nil
}

func roundUpToMultiple(value, multiple int64) int64 {
	if multiple <= 0 {
		return value
	}
	remainder := value % multiple
	if remainder == 0 {
		return value
	}
	return value + multiple - remainder
}

type countingReader struct {
	r io.Reader
	n int64
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.n += int64(n)
	return n, err
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
	batches := make([][]string, 0, (len(keys)+999)/1000)
	for len(keys) > 0 {
		batch := keys
		if len(batch) > 1000 {
			batch = keys[:1000]
		}
		batches = append(batches, batch)
		keys = keys[len(batch):]
	}
	if len(batches) == 0 {
		return nil
	}
	workerCount := dataObjectDeleteConcurrency
	if workerCount > len(batches) {
		workerCount = len(batches)
	}
	jobs := make(chan []string)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()
		for batch := range jobs {
			if err := b.deleteObjectBatch(ctx, batch); err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
		}
	}
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go worker()
	}
	for _, batch := range batches {
		select {
		case err := <-errCh:
			close(jobs)
			wg.Wait()
			return err
		case jobs <- batch:
		}
	}
	close(jobs)
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func (b *dataObjectBackend) deleteObjectBatch(ctx context.Context, batch []string) error {
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
