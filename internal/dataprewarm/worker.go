// Package dataprewarm implements a bounded, content-addressed runtime cache.
// It never interprets manifest paths as filesystem paths or changes app mounts.
package dataprewarm

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"fugue/internal/model"
)

type Blob struct {
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
	URL    string `json:"url"`
}
type Plan struct {
	TransferID string             `json:"transfer_id"`
	Manifest   model.DataManifest `json:"manifest"`
	Blobs      []Blob             `json:"blobs"`
	ExpiresAt  time.Time          `json:"expires_at"`
}
type Progress struct {
	TransferID     string    `json:"transfer_id"`
	ManifestDigest string    `json:"manifest_digest"`
	State          string    `json:"state"`
	BytesDone      int64     `json:"bytes_done"`
	FilesDone      int       `json:"files_done"`
	ObservedAt     time.Time `json:"observed_at"`
}

var digestPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)

func Validate(plan Plan) error {
	if plan.TransferID == "" || plan.ExpiresAt.IsZero() || !digestPattern.MatchString(plan.Manifest.Digest) {
		return fmt.Errorf("invalid prewarm plan identity")
	}
	if ManifestDigest(plan.Manifest) != plan.Manifest.Digest {
		return fmt.Errorf("manifest digest mismatch")
	}
	seen := map[string]int64{}
	for _, blob := range plan.Blobs {
		if !digestPattern.MatchString(blob.SHA256) || blob.Size < 0 || blob.Size > 10<<30 || blob.URL == "" {
			return fmt.Errorf("invalid prewarm blob")
		}
		if _, exists := seen[blob.SHA256]; exists {
			return fmt.Errorf("duplicate prewarm blob")
		}
		seen[blob.SHA256] = blob.Size
	}
	var total int64
	count := 0
	for _, entry := range plan.Manifest.Entries {
		if entry.Kind != "file" {
			continue
		}
		if size, ok := seen[entry.SHA256]; !ok || size != entry.Size {
			return fmt.Errorf("manifest blob missing or size differs")
		}
		total += entry.Size
		count++
	}
	if total != plan.Manifest.TotalBytes || count != plan.Manifest.FileCount || total > 10<<30 {
		return fmt.Errorf("manifest totals are inconsistent or exceed the 10 GiB cache limit")
	}
	if len(plan.Manifest.Entries) > 10000 {
		return fmt.Errorf("prewarm supports at most 10000 manifest entries")
	}
	return nil
}

// Run returns only after all declared blob hashes and byte counts match. The
// ready receipt is written last; partial data has no ready marker.
func Run(ctx context.Context, plan Plan, cache string, emit func(Progress)) (err error) {
	if err = Validate(plan); err != nil {
		return err
	}
	if !plan.ExpiresAt.After(time.Now()) {
		return fmt.Errorf("prewarm plan expired")
	}
	ctx, cancel := context.WithDeadline(ctx, minTime(plan.ExpiresAt, time.Now().Add(30*time.Minute)))
	defer cancel()
	blobsDir := filepath.Join(cache, "blobs")
	if err = os.MkdirAll(blobsDir, 0700); err != nil {
		return err
	}
	// This directory is dedicated to a single transfer PVC; only our two output
	// names are removed on failure. Successful source snapshots remain untouched.
	defer func() {
		if err != nil {
			_ = os.RemoveAll(blobsDir)
			_ = os.Remove(filepath.Join(cache, "ready.json"))
			_ = os.Remove(filepath.Join(cache, "manifest.json"))
		}
	}()
	progress := Progress{TransferID: plan.TransferID, ManifestDigest: plan.Manifest.Digest, State: "downloading"}
	send := func() {
		progress.ObservedAt = time.Now().UTC()
		if emit != nil {
			emit(progress)
		}
	}
	send()
	client := &http.Client{Timeout: 10 * time.Minute, CheckRedirect: func(*http.Request, []*http.Request) error { return fmt.Errorf("blob redirects are not allowed") }}
	weights := map[string]struct {
		bytes int64
		files int
	}{}
	for _, entry := range plan.Manifest.Entries {
		if entry.Kind == "file" {
			w := weights[entry.SHA256]
			w.bytes += entry.Size
			w.files++
			weights[entry.SHA256] = w
		}
	}

	for _, blob := range plan.Blobs {
		if err = ctx.Err(); err != nil {
			return err
		}
		tmp := filepath.Join(blobsDir, blob.SHA256+".partial")
		file, openErr := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
		if openErr != nil {
			return openErr
		}
		req, reqErr := http.NewRequestWithContext(ctx, http.MethodGet, blob.URL, nil)
		if reqErr != nil {
			file.Close()
			return fmt.Errorf("invalid blob URL")
		}
		resp, readErr := client.Do(req)
		if readErr != nil {
			file.Close()
			return fmt.Errorf("blob download interrupted")
		}
		if resp.StatusCode != 200 {
			resp.Body.Close()
			file.Close()
			return fmt.Errorf("blob download status %d", resp.StatusCode)
		}
		h := sha256.New()
		n, copyErr := io.Copy(io.MultiWriter(file, h), io.LimitReader(resp.Body, blob.Size+1))
		resp.Body.Close()
		syncErr := file.Sync()
		closeErr := file.Close()
		if copyErr != nil || syncErr != nil || closeErr != nil || n != blob.Size || fmt.Sprintf("%x", h.Sum(nil)) != blob.SHA256 {
			return fmt.Errorf("blob size, digest, or durable write verification failed")
		}
		if err = os.Rename(tmp, filepath.Join(blobsDir, blob.SHA256)); err != nil {
			return err
		}
		progress.BytesDone += weights[blob.SHA256].bytes
		progress.FilesDone += weights[blob.SHA256].files
		send()
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	manifest, err := json.Marshal(plan.Manifest)
	if err != nil {
		return err
	}
	if err = writeDurable(filepath.Join(cache, "manifest.json"), manifest); err != nil {
		return err
	}
	progress.State = "ready"
	progress.ObservedAt = time.Now().UTC()
	receipt, err := json.Marshal(progress)
	if err != nil {
		return err
	}
	if err = writeDurable(filepath.Join(cache, "ready.json"), receipt); err != nil {
		return err
	}
	send()
	return nil
}
func writeDurable(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err = f.Write(data); err != nil {
		return err
	}
	return f.Sync()
}
func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func ManifestDigest(manifest model.DataManifest) string {
	manifest = model.NormalizeDataManifest(manifest)
	raw, _ := json.Marshal(manifest.Entries)
	return fmt.Sprintf("%x", sha256.Sum256(raw))
}
