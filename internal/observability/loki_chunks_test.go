package observability

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestLokiExporterKeepsOversizedEventsWithinServerLimitWithoutLosingContent(t *testing.T) {
	for _, oversized := range []Event{
		{Message: strings.Repeat("log output ", 50000)},
		{Message: strings.Repeat("界🙂\"\\\n\u0000<>&", 40000)},
		{Message: "structured output", Attributes: map[string]string{"result": strings.Repeat("data", 100000)}},
	} {
		t.Run(fmt.Sprintf("message-%d-attributes-%d", len(oversized.Message), len(oversized.Attributes)), func(t *testing.T) {
			oversized.Timestamp = time.Unix(10, 20).UTC()
			oversized.Kind = EventKindLog
			oversized.Source = "kubernetes:synthetic"
			if oversized.Attributes == nil {
				oversized.Attributes = map[string]string{}
			}
			oversized.Attributes["component"] = "synthetic"
			oversized.Attributes["app_id"] = "app_synthetic"
			ordinary := Event{Timestamp: oversized.Timestamp.Add(time.Second), Kind: EventKindLog, Message: "still exported", Attributes: map[string]string{"component": "synthetic", "app_id": "app_synthetic"}}
			original, _ := json.Marshal(lokiLogLine(oversized))
			ordinaryJSON, _ := json.Marshal(lokiLogLine(ordinary))
			var received []lokiPushRequest
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var payload lokiPushRequest
				if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
					t.Error(err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				for _, stream := range payload.Streams {
					for _, value := range stream.Values {
						if len(value[1]) > 262144 {
							w.WriteHeader(http.StatusBadRequest)
							return
						}
					}
				}
				received = append(received, payload)
				w.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()
			exporter := LokiExporter{Client: server.Client(), PushURL: server.URL}
			for range 2 { // Retries must produce identical timestamps, IDs and fragments.
				if err := exporter.Export(context.Background(), []Event{oversized, ordinary}); err != nil {
					t.Fatal(err)
				}
			}
			if len(received) != 2 || !reflect.DeepEqual(received[0], received[1]) {
				t.Fatal("export must be deterministic on retry")
			}
			if len(received[0].Streams) != 1 || !reflect.DeepEqual(received[0].Streams[0].Stream, lokiLabels(oversized)) {
				t.Fatal("chunking changed stream identity")
			}
			values := received[0].Streams[0].Values
			if len(values) < 3 || values[len(values)-1][1] != string(ordinaryJSON) {
				t.Fatal("ordinary event was lost or changed")
			}
			var reconstructed strings.Builder
			for i, value := range values[:len(values)-1] {
				var chunk lokiLine
				if !utf8.ValidString(value[1]) || json.Unmarshal([]byte(value[1]), &chunk) != nil {
					t.Fatal("fragment is not valid UTF-8 JSON")
				}
				if value[0] != strconv.FormatInt(oversized.Timestamp.UnixNano(), 10) || chunk.Timestamp != oversized.Timestamp.Format(time.RFC3339Nano) {
					t.Fatal("chunking changed event timestamp")
				}
				if chunk.Attributes["log_chunk_index"] != strconv.Itoa(i) || chunk.Attributes["log_chunk_count"] != strconv.Itoa(len(values)-1) || chunk.Attributes["log_chunk_encoding"] != "loki-line-json-v1" || chunk.Attributes["log_chunk_sha256"] != fmt.Sprintf("%x", sha256.Sum256(original)) {
					t.Fatal("invalid reconstruction metadata")
				}
				reconstructed.WriteString(chunk.Message)
			}
			if reconstructed.String() != string(original) {
				t.Fatal("oversized event lost content during chunking")
			}
		})
	}
}

func TestLokiLogLinesExactByteBoundary(t *testing.T) {
	event := Event{Kind: EventKindLog, Message: "x"}
	line, _ := json.Marshal(lokiLogLine(event))
	for _, size := range []int{262143, 262144, 262145} {
		event.Message = strings.Repeat("x", size-len(line)+1)
		lines, err := lokiLogLines(event)
		if err != nil {
			t.Fatal(err)
		}
		if (len(lines) == 1) != (size <= 262144) {
			t.Fatalf("unexpected chunk count at %d bytes: %d", size, len(lines))
		}
	}
}
