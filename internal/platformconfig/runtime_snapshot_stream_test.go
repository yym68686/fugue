package platformconfig

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

func TestRuntimeSnapshotStreamPreservesV1BytesAndDigest(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 123456789, time.FixedZone("offset", 3600))
	for _, snapshot := range []RuntimeSnapshot{
		{},
		{IsolateTrafficAvailability: true},
		{RetainedTrafficPartitions: []RetainedTrafficPartition{{AppID: "synthetic-app", InputDigest: "sha256:synthetic", ObservedAt: now, ExpiresAt: now.Add(time.Minute), Routes: []CompiledRoute{{RouteIntent: RouteIntent{Hostname: "example.test", AppID: "synthetic-app"}}}}}},
		{IntentGeneration: "<&\u2028\x00", PolicyGeneration: "policy", CapturedAt: &now, Facts: map[string]any{"z": nil, "a": []any{1.25, "中文", map[string]any{"a": true}}, "invalid\xff": -0.001}},
		{DNSSelections: []DNSSelectionObservation{{NodeID: "node-a", Hostname: "app.example.test", PhysicalEvidence: json.RawMessage(`{"snapshot":{"z":1,"a":{"b":2,"a":3}},"result":{"value":4}}`)}, {NodeID: "node-b"}}, Origins: []OriginObservation{{Ref: "origin-a", ObservedAt: now}}, Releases: []ReleaseObservation{{ID: "release-a", ObservedAt: now}}},
	} {
		want, err := EncodeRuntimeSnapshot(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		var got bytes.Buffer
		if err := WriteRuntimeSnapshot(&got, snapshot); err != nil || !bytes.Equal(got.Bytes(), want) {
			t.Fatalf("v1 bytes changed: %v\ngot %s\nwant %s", err, got.Bytes(), want)
		}
		h := sha256.Sum256(want)
		digest, err := RuntimeSnapshotDigest(snapshot)
		if err != nil || digest != "sha256:"+hex.EncodeToString(h[:]) {
			t.Fatal("v1 digest changed", digest, err)
		}
		// Canonicalizing raw evidence must not mutate the caller's snapshot.
		after, _ := EncodeRuntimeSnapshot(snapshot)
		if !bytes.Equal(want, after) {
			t.Fatal("input mutated")
		}
		decoded, err := DecodeRuntimeSnapshotReader(bytes.NewReader(want))
		if err != nil {
			t.Fatal(err)
		}
		actual, err := RuntimeSnapshotDigest(decoded)
		legacyDecoded, legacyErr := DecodeRuntimeSnapshotJSON(want)
		legacyDigest, legacyDigestErr := RuntimeSnapshotDigest(legacyDecoded)
		if err != nil || legacyErr != nil || legacyDigestErr != nil || actual != legacyDigest {
			t.Fatal("streaming decode changed canonical digest", err)
		}
	}
}

type shortSnapshotWriter struct{}

func (shortSnapshotWriter) Write(b []byte) (int, error) { return 0, nil }
func TestRuntimeSnapshotStreamRejectsWriteAndEncodingFailures(t *testing.T) {
	if err := WriteRuntimeSnapshot(shortSnapshotWriter{}, RuntimeSnapshot{}); !errors.Is(err, io.ErrShortWrite) {
		t.Fatal(err)
	}
	for _, s := range []RuntimeSnapshot{{Facts: map[string]any{"invalid": make(chan int)}}, {DNSSelections: []DNSSelectionObservation{{PhysicalEvidence: json.RawMessage("{")}}}} {
		if err := WriteRuntimeSnapshot(io.Discard, s); err == nil {
			t.Fatal("invalid value accepted")
		}
	}
	for _, raw := range []string{`{"unexpected":true}`, `{"origins":[{"unknown":true}]}`, `{"origins":1}`, `{"intent_generation":"a","intent_generation":"b"}`, `{} {}`, `[]`, `{"origins":[`} {
		if _, err := DecodeRuntimeSnapshotReader(strings.NewReader(raw)); err == nil {
			t.Fatal("invalid streamed snapshot accepted", raw)
		}
	}
}

func FuzzRuntimeSnapshotStreamMatchesV1(f *testing.F) {
	f.Add([]byte(`{"intent_generation":"a","policy_generation":"b","facts":{"escaped":"<&","v":[true,1.5,null]}}`))
	f.Add([]byte(`{"dns_selections":[{"node_id":"node-a"}],"origins":[{"ref":"origin-a"}],"dns_consumers":[{"node_id":"node-b"}]}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		var s RuntimeSnapshot
		if json.Unmarshal(raw, &s) != nil {
			return
		}
		want, wantErr := EncodeRuntimeSnapshot(s)
		var got bytes.Buffer
		err := WriteRuntimeSnapshot(&got, s)
		if (err == nil) != (wantErr == nil) || err == nil && !bytes.Equal(want, got.Bytes()) {
			t.Fatalf("v1 encoding differs: %v / %v", err, wantErr)
		}
	})
}

func BenchmarkRuntimeSnapshotDigestStreaming(b *testing.B) {
	s := RuntimeSnapshot{Facts: map[string]any{}}
	for i := 0; i < 256; i++ {
		s.Origins = append(s.Origins, OriginObservation{Ref: strings.Repeat("x", 256<<10)})
	}
	for _, streaming := range []bool{false, true} {
		name := "whole-buffer"
		if streaming {
			name = "streaming"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if streaming {
					if _, err := RuntimeSnapshotDigest(s); err != nil {
						b.Fatal(err)
					}
				} else {
					raw, err := EncodeRuntimeSnapshot(s)
					if err != nil {
						b.Fatal(err)
					}
					_ = sha256.Sum256(raw)
				}
			}
		})
	}
}
