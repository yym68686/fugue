package objectstorage

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Explicit opt-in. Creates uniquely named disposable buckets and credentials,
// verifies real S3 authorization, and removes only its own test resources.
func TestR2ObjectStorageIntegration(t *testing.T) {
	if os.Getenv("FUGUE_S3_INTEGRATION") != "1" {
		t.Skip("set FUGUE_S3_INTEGRATION=1 with R2 management credentials")
	}
	c := New(os.Getenv("FUGUE_DATA_R2_ACCOUNT_ID"), os.Getenv("FUGUE_S3_CF_API_TOKEN"))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := c.Verify(ctx); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	suffix := hex.EncodeToString(b)
	bucket := "fugue-s3-test-" + suffix
	if err := c.EnsureBucket(ctx, bucket); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := c.call(context.Background(), "DELETE", "/r2/buckets/"+bucket, nil, nil); err != nil {
			t.Errorf("cleanup bucket %s: %v", bucket, err)
		}
	}()
	tokenName := "fugue-s3-test-" + suffix
	defer func() {
		if err := c.RevokeNamed(context.Background(), tokenName); err != nil {
			t.Errorf("cleanup named credential: %v", err)
		}
	}()
	token, err := c.CreateCredential(ctx, tokenName, bucket, "read-write")
	if err != nil {
		t.Fatal(err)
	}
	keyValue := token.Value
	cli := s3.New(s3.Options{Region: "auto", BaseEndpoint: aws.String("https://" + c.AccountID + ".r2.cloudflarestorage.com"), UsePathStyle: true, Credentials: credentials.NewStaticCredentialsProvider(token.ID, SecretAccessKey(keyValue), "")})
	var putErr error
	for i := 0; i < 12; i++ {
		_, putErr = cli.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String("probe"), Body: bytes.NewBufferString("object-storage-probe")})
		if putErr == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if putErr != nil {
		t.Fatal("scoped S3 write failed", putErr)
	}
	defer func() {
		_, _ = cli.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String("probe")})
	}()
	get, err := cli.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String("probe")})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(get.Body)
	_ = get.Body.Close()
	if err != nil || string(raw) != "object-storage-probe" {
		t.Fatal("round trip mismatch")
	}
	_, err = cli.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String("probe")})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Revoke(ctx, token.ID); err != nil {
		t.Fatal(err)
	}
	revoked := false
	for i := 0; i < 15; i++ {
		_, err = cli.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
		if err != nil {
			revoked = true
			break
		}
		time.Sleep(time.Second)
	}
	if !revoked {
		t.Fatal("revoked credential still accesses bucket")
	}
	t.Log("R2 bucket provisioning, S3 credentials, round trip and revocation verified")
}
