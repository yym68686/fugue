package inputarchive

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
)

const PendingRootHTTPPath = "/v1/platform-state/compiler-inputs/pending-roots"
const PendingRootAudience = "fugue.compiler-input.pending-root.v1"
const PendingRootHTTPBytes = 24 << 20

var ErrPendingRootTooLarge = errors.New("compiler input pending retention request exceeds bound")

// A canonical outer envelope rejects duplicate/unknown fields and preserves
// every original source byte. Source authentication remains the independent
// source verifier's responsibility, not this transport codec's.
type pendingRootHTTPEnvelope struct {
	Schema       string                  `json:"schema"`
	Registration PendingRootRegistration `json:"registration"`
	SourceSchema string                  `json:"source_schema"`
	SourceBase64 string                  `json:"source_base64"`
}

func MarshalPendingRootHTTP(r PendingRootRequest) ([]byte, error) {
	if _, _, err := r.Canonical(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(pendingRootHTTPEnvelope{r.Schema, r.Registration, r.SourceSchema, base64.StdEncoding.EncodeToString(r.Source)})
	if err != nil || len(raw) > PendingRootHTTPBytes {
		return nil, ErrPendingRootTooLarge
	}
	return raw, nil
}

func DecodePendingRootHTTP(raw []byte) (PendingRootRequest, error) {
	if len(raw) > PendingRootHTTPBytes {
		return PendingRootRequest{}, ErrPendingRootTooLarge
	}
	var wire pendingRootHTTPEnvelope
	if json.Unmarshal(raw, &wire) != nil {
		return PendingRootRequest{}, ErrPendingRoot
	}
	canonical, err := json.Marshal(wire)
	if err != nil || !bytes.Equal(raw, canonical) {
		return PendingRootRequest{}, ErrPendingRoot
	}
	if len(wire.SourceBase64) > base64.StdEncoding.EncodedLen(PendingRootSourceBytes) {
		return PendingRootRequest{}, ErrPendingRootTooLarge
	}
	source, err := base64.StdEncoding.Strict().DecodeString(wire.SourceBase64)
	if err != nil || base64.StdEncoding.EncodeToString(source) != wire.SourceBase64 {
		return PendingRootRequest{}, ErrPendingRoot
	}
	if len(source) > PendingRootSourceBytes {
		return PendingRootRequest{}, ErrPendingRootTooLarge
	}
	r := PendingRootRequest{Schema: wire.Schema, Registration: wire.Registration, SourceSchema: wire.SourceSchema, Source: source}
	if _, _, err := r.Canonical(); err != nil {
		return PendingRootRequest{}, err
	}
	return r, nil
}
