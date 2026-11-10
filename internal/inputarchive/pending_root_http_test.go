package inputarchive

import (
	"bytes"
	"encoding/base64"
	"strings"
	"testing"
)

func TestPendingRootHTTPPreservesSourceAndRejectsAmbiguousEnvelope(t *testing.T) {
	r := PendingRootRequest{Schema: PendingRootRequestSchema, SourceSchema: "original-source-v1", Source: []byte("{\n \"original\": \"<signed>&\"\n}"),
		Registration: PendingRootRegistration{Schema: PendingRootSchema, Owner: "authority/neutral", Generation: "neutral", SourceDigest: "sha256:" + strings.Repeat("a", 64), References: []HotReference{{Kind: "release", ID: "neutral-release"}}}}
	raw, err := MarshalPendingRootHTTP(r)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodePendingRootHTTP(raw)
	if err != nil || !bytes.Equal(decoded.Source, r.Source) {
		t.Fatal("original bytes changed", err)
	}
	for _, mutate := range []func(string) string{
		func(s string) string { return " " + s },
		func(s string) string { return s + "{}" },
		func(s string) string { return strings.Replace(s, `"schema":`, `"unknown":0,"schema":`, 1) },
		func(s string) string { return strings.Replace(s, `"owner":`, `"owner":"forged","owner":`, 1) },
		func(s string) string { return strings.Replace(s, `"id":`, `"unrecognized":true,"id":`, 1) },
		func(s string) string {
			return strings.Replace(s, base64.StdEncoding.EncodeToString(r.Source), "e30=\\n", 1)
		},
		func(s string) string {
			return strings.Replace(s, base64.StdEncoding.EncodeToString(r.Source), "e31=", 1)
		},
		func(s string) string { return strings.Replace(s, `"references":[`, `"references":null,"ignored":[`, 1) },
	} {
		if _, err := DecodePendingRootHTTP([]byte(mutate(string(raw)))); err == nil {
			t.Fatal("ambiguous request accepted")
		}
	}
	if _, err := DecodePendingRootHTTP(bytes.Repeat([]byte{' '}, PendingRootHTTPBytes+1)); err != ErrPendingRootTooLarge {
		t.Fatal(err)
	}
}
