package platformconfig

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// Keep the original v1 canonicalizer as an independent compatibility oracle.
func legacyCanonicalPhysicalEvidence(raw json.RawMessage) (json.RawMessage, error) {
	if !json.Valid(raw) {
		return nil, errors.New("invalid physical evidence JSON")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var value any
	if err := d.Decode(&value); err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

func legacyEvidenceSnapshot(snapshot RuntimeSnapshot) ([]byte, error) {
	snapshot.DNSSelections = append([]DNSSelectionObservation(nil), snapshot.DNSSelections...)
	for i := range snapshot.DNSSelections {
		fact := &snapshot.DNSSelections[i]
		for _, raw := range []*json.RawMessage{&fact.RetainedPhysicalEvidence, &fact.PhysicalEvidence} {
			if len(*raw) == 0 {
				continue
			}
			canonical, err := legacyCanonicalPhysicalEvidence(*raw)
			if err != nil {
				return nil, err
			}
			*raw = canonical
		}
	}
	return json.Marshal(snapshot)
}

func FuzzRuntimeSnapshotPhysicalEvidenceV1(f *testing.F) {
	for _, raw := range []string{
		` {"z":[-0,9007199254740993,1e+10000],"a":"<&\u2028","a":"last","\u0061":"last again"} ` + "\t\r\n",
		"null", "false", "[]", "{},", `{} {}`, "1\v", "1\u00a0", `1e`, `{"a":`, "\"\xff\"", "", "  ",
	} {
		f.Add([]byte(raw), []byte(raw))
		f.Add([]byte(raw), []byte(`{"result":true}`))
		f.Add([]byte(raw), []byte(nil))
	}
	f.Fuzz(func(t *testing.T, physical, retained []byte) {
		snapshot := RuntimeSnapshot{DNSSelections: []DNSSelectionObservation{{
			NodeID: "test-dns", SourceDigest: "sha256:" + strings.Repeat("a", 64), PhysicalEvidence: physical, RetainedPhysicalEvidence: retained,
		}}}
		// The same claimed digest would not justify reusing distinct evidence.
		beforePhysical, beforeRetained := bytes.Clone(physical), bytes.Clone(retained)
		want, wantErr := legacyEvidenceSnapshot(snapshot)
		got, err := EncodeRuntimeSnapshot(snapshot)
		var out bytes.Buffer
		writeErr := WriteRuntimeSnapshot(&out, snapshot)
		if wantErr != nil {
			if err == nil || writeErr == nil {
				t.Fatal("invalid evidence accepted")
			}
		} else {
			if err != nil || writeErr != nil || !bytes.Equal(got, want) || !bytes.Equal(out.Bytes(), want) {
				t.Fatal("changed v1 encoding", err, writeErr)
			}
			sum := sha256.Sum256(want)
			digest, err := RuntimeSnapshotDigest(snapshot)
			if err != nil || digest != "sha256:"+hex.EncodeToString(sum[:]) {
				t.Fatal("changed v1 digest", err)
			}
		}
		fact := snapshot.DNSSelections[0]
		if !bytes.Equal(beforePhysical, fact.PhysicalEvidence) || !bytes.Equal(beforeRetained, fact.RetainedPhysicalEvidence) {
			t.Fatal("modified input evidence")
		}
	})
}

func BenchmarkRuntimeSnapshotPairedEvidence(b *testing.B) {
	candidates := strings.Repeat(`{"weight":21,"address":"192.0.2.1","proofs":[{"latency_ms":10,"path":"edge.example.test"}]},`, 99)
	snapshot := RuntimeSnapshot{}
	for i := range 32 {
		raw := json.RawMessage(fmt.Sprintf(`{"snapshot":{"candidates":[%s{}]},"result":{"label":"host-%d"}}`, candidates, i))
		snapshot.DNSSelections = append(snapshot.DNSSelections, DNSSelectionObservation{PhysicalEvidence: raw, RetainedPhysicalEvidence: bytes.Clone(raw)})
	}
	for _, test := range []struct {
		name string
		fn   func(RuntimeSnapshot) ([]byte, error)
	}{{"previous", legacyEvidenceSnapshot}, {"paired", EncodeRuntimeSnapshot}} {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := test.fn(snapshot); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
