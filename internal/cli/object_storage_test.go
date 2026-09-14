package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestObjectStorageEnvSecretsArePrivateAndNeverOverwrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "connection.env")
	env := map[string]string{"AWS_SECRET_ACCESS_KEY": "synthetic-secret", "S3_BUCKET": "private-bucket"}
	if err := writePrivateObjectStorageEnv(path, env); err != nil {
		t.Fatal(err)
	}
	stat, err := os.Stat(path)
	if err != nil || stat.Mode().Perm() != 0600 {
		t.Fatal("connection permissions must be 0600")
	}
	if err = writePrivateObjectStorageEnv(path, map[string]string{"S3_BUCKET": "overwritten"}); err == nil {
		t.Fatal("overwrote an existing connection")
	}
	data, _ := os.ReadFile(path)
	if !strings.Contains(string(data), "private-bucket") {
		t.Fatal("existing connection changed")
	}
	if err = writePrivateObjectStorageEnv(filepath.Join(t.TempDir(), "bad.env"), map[string]string{"S3_BUCKET": "bucket\nINJECTED=value"}); err == nil {
		t.Fatal("accepted newline injection")
	}
}
