package api

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestAssignedArtifactContentCodingPreservesEntireEnvelope(t *testing.T) {
	for _, tc := range []struct {
		accept, kind string
		compressed   bool
	}{
		{"gzip", model.PlatformArtifactKindDNSAnswerBundle, true},
		{"GZip", model.PlatformArtifactKindDNSAnswerBundle, true},
		{"", model.PlatformArtifactKindDNSAnswerBundle, false},
		{"gzip;q=0", model.PlatformArtifactKindDNSAnswerBundle, false},
		{"gzip, gzip;q=0", model.PlatformArtifactKindDNSAnswerBundle, false},
		{"br", model.PlatformArtifactKindDNSAnswerBundle, false},
		{"gzip", model.PlatformArtifactKindReleaseSet, false},
		{"gzip", model.PlatformArtifactKindCaddyRouteConfig, false},
	} {
		t.Run(tc.kind+"/"+tc.accept, func(t *testing.T) {
			value := consumerArtifactLookup{Artifact: model.PlatformArtifact{ArtifactKind: tc.kind, ID: "signed-child", ContentHash: "digest", Content: map[string]any{"rows": strings.Repeat("record-with-lease;", 5000)}}, Assignment: model.PlatformConsumerAssignment{ExpectedConsumerSetID: "set", Revision: 3}, Release: model.PlatformArtifactRelease{ID: "full", FencingToken: 8}}
			var want bytes.Buffer
			if err := json.NewEncoder(&want).Encode(value); err != nil {
				t.Fatal(err)
			}
			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			if tc.accept != "" {
				r.Header.Set("Accept-Encoding", tc.accept)
			}
			writeAssignedConsumerArtifact(w, r, value)
			raw := w.Body.Bytes()
			if w.Code != http.StatusOK || w.Header().Get("Content-Type") != "application/json" || w.Header().Get("Cache-Control") != "private, no-store" {
				t.Fatal(w.Code, w.Header())
			}
			if (w.Header().Get("Vary") == "Accept-Encoding") != (tc.kind == model.PlatformArtifactKindDNSAnswerBundle) {
				t.Fatal("wrong negotiated response", w.Header())
			}
			if got := w.Header().Get("Content-Encoding"); (got == "gzip") != tc.compressed {
				t.Fatal(got)
			}
			if tc.compressed {
				if len(raw) >= want.Len()/2 || w.Header().Get("ETag") == `"digest"` {
					t.Fatal("not a separate compressed representation")
				}
				reader, err := gzip.NewReader(bytes.NewReader(raw))
				if err != nil {
					t.Fatal(err)
				}
				raw, err = io.ReadAll(reader)
				if err != nil {
					t.Fatal(err)
				}
				if err = reader.Close(); err != nil {
					t.Fatal(err)
				}
			} else if w.Header().Get("ETag") != `"digest"` {
				t.Fatal("legacy identity tag changed")
			}
			if !bytes.Equal(raw, want.Bytes()) {
				t.Fatal("signed envelope changed")
			}
		})
	}
}

func TestAssignedDNSArtifactGoTransportAndContention(t *testing.T) {
	value := consumerArtifactLookup{Artifact: model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindDNSAnswerBundle, ContentHash: "one-digest", Content: map[string]any{"signed": "same"}}, Assignment: model.PlatformConsumerAssignment{ExpectedConsumerSetID: "one-set"}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeAssignedConsumerArtifact(w, r, value)
	}))
	defer srv.Close()
	for _, busy := range []bool{false, true} {
		func() {
			if busy {
				for i := 0; i < cap(signedDNSGzipActive); i++ {
					signedDNSGzipActive <- struct{}{}
				}
				defer func() {
					for i := 0; i < cap(signedDNSGzipActive); i++ {
						<-signedDNSGzipActive
					}
				}()
			}
			resp, err := srv.Client().Get(srv.URL)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			var decoded consumerArtifactLookup
			if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil || decoded.Assignment.ExpectedConsumerSetID != "one-set" || decoded.Artifact.Content["signed"] != "same" || resp.Uncompressed == busy {
				t.Fatal("transport hid or changed an assigned representation", err, resp.Uncompressed, busy)
			}
		}()
	}
}
