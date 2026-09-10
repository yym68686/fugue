package api

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func TestBackupRangeVerificationRetriesOnlyInterruptedSegment(t *testing.T) {
	for _, mode := range []string{"retry", "persistent-truncation", "corruption", "wrong-range", "changed-etag", "ignored-range", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			payload := []byte("abcdefghijklm")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var mu sync.Mutex
			counts := map[int]int{}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("ETag", `"object-version"`)
				if r.Method == http.MethodHead {
					w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
					return
				}
				if r.Header.Get("If-Match") != `"object-version"` {
					t.Error("range request is not bound to the inspected object")
				}
				var start, end int
				if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil || start < 0 || end >= len(payload) {
					t.Errorf("invalid range: %q", r.Header.Get("Range"))
					http.Error(w, "invalid range", 400)
					return
				}
				mu.Lock()
				counts[start]++
				attempt := counts[start]
				mu.Unlock()
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(payload)))
				w.Header().Set("Content-Length", strconv.Itoa(end-start+1))
				body := append([]byte(nil), payload[start:end+1]...)
				if start == 5 {
					switch mode {
					case "retry":
						if attempt == 1 {
							body = body[:2]
						}
					case "persistent-truncation":
						body = body[:2]
					case "corruption":
						body[0] ^= 1
					case "wrong-range":
						w.Header().Set("Content-Range", "bytes 0-4/13")
					case "changed-etag":
						w.Header().Set("ETag", `"different-version"`)
					case "ignored-range":
						w.Header().Del("Content-Range")
					case "cancel":
						cancel()
					}
				}
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(body)
			}))
			defer server.Close()
			backend := newTestDataObjectBackend(t, server.URL)
			digest := fmt.Sprintf("%x", sha256.Sum256(payload))
			err := verifyBackupObjectSHA256Ranges(ctx, backend, "backup", int64(len(payload)), digest, 5)
			mu.Lock()
			defer mu.Unlock()
			switch mode {
			case "retry":
				if err != nil || counts[0] != 1 || counts[5] != 2 || counts[10] != 1 {
					t.Fatalf("retry must preserve completed segments and exact digest: %v, requests=%v", err, counts)
				}
			case "persistent-truncation":
				if err == nil || counts[5] != 3 || counts[10] != 0 || !strings.Contains(err.Error(), "read 2 bytes") {
					t.Fatalf("expected bounded retries and failure evidence, got %v, requests=%v", err, counts)
				}
			case "corruption":
				if err == nil || !strings.Contains(err.Error(), "sha256 mismatch") {
					t.Fatalf("corrupt object must fail final checksum: %v", err)
				}
			case "cancel":
				if !errors.Is(err, context.Canceled) || counts[5] != 1 || counts[10] != 0 {
					t.Fatalf("cancellation must stop retries: %v, requests=%v", err, counts)
				}
			default:
				if !errors.Is(err, errBackupRangeMetadata) || counts[5] != 1 || counts[10] != 0 {
					t.Fatalf("invalid metadata must stop verification: %v, requests=%v", err, counts)
				}
			}
		})
	}
}

func TestBackupVerificationPreservesSmallAndUnknownSizeReads(t *testing.T) {
	for _, expected := range []int64{4, -1, 0} {
		t.Run(strconv.FormatInt(expected, 10), func(t *testing.T) {
			payload := "dump"
			if expected == 0 {
				payload = ""
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.Header.Get("Range") != "" {
					t.Error("small read unexpectedly required HEAD/Range")
				}
				w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
				_, _ = w.Write([]byte(payload))
			}))
			defer server.Close()
			err := verifyBackupObjectSHA256(context.Background(), newTestDataObjectBackend(t, server.URL), "backup", expected, fmt.Sprintf("%x", sha256.Sum256([]byte(payload))))
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestBackupVerificationLargeObjectChecksMetadataBeforeReading(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Error("large backup attempted an unbounded read")
		}
		w.Header().Set("Content-Length", "4")
		w.Header().Set("ETag", `"object-version"`)
	}))
	defer server.Close()
	err := verifyBackupObjectSHA256(context.Background(), newTestDataObjectBackend(t, server.URL), "backup", dataMultipartPartSize+1, strings.Repeat("0", 64))
	if err == nil || !strings.Contains(err.Error(), "metadata size mismatch") {
		t.Fatalf("must reject wrong object size: %v", err)
	}
}

func TestBackupVerificationTruncatedSmallObjectFallsBackToBoundRanges(t *testing.T) {
	payload := "complete-backup"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
		w.Header().Set("ETag", `"object-version"`)
		if r.Method == http.MethodHead {
			return
		}
		if r.Header.Get("Range") == "" {
			_, _ = w.Write([]byte(payload[:3]))
			return
		}
		if r.Header.Get("If-Match") != `"object-version"` {
			t.Error("missing version constraint")
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(payload)-1, len(payload)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte(payload))
	}))
	defer server.Close()
	err := verifyBackupObjectSHA256(context.Background(), newTestDataObjectBackend(t, server.URL), "backup", int64(len(payload)), fmt.Sprintf("%x", sha256.Sum256([]byte(payload))))
	if err != nil {
		t.Fatal(err)
	}
}
