// Package dnsroutewire defines the byte-bound validator of a source observation.
package dnsroutewire

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"strconv"
	"time"

	"fugue/internal/model"
)

const ObservedAtHeader = "X-Fugue-Observed-At"

// Validator includes all actual content and authority envelopes. Only the
// observation time is excluded; it must be supplied freshly on every recheck.
// It is a weak HTTP validator because that one response field changes.
func Validator(response model.PlatformConsumerDNSRouteSourcesResponse, retained *model.PlatformLKGSnapshot) (string, error) {
	response.Snapshot.ObservedAt = time.Time{}
	value := model.PlatformConsumerRetainedDNSRouteSourcesResponse{PlatformConsumerDNSRouteSourcesResponse: response}
	if retained != nil {
		value.RetainedLKG = *retained
	}
	h := &sourceDigestWriter{Hash: sha256.New()}
	if err := json.NewEncoder(h).Encode(value); err != nil {
		return "", err
	}
	return "W/" + strconv.Quote("dns-source-v1:"+hex.EncodeToString(h.Sum(nil))), nil
}

type sourceDigestWriter struct {
	hash.Hash
	bytes int
}

func (w *sourceDigestWriter) Write(raw []byte) (int, error) {
	if len(raw) > (16<<20)-w.bytes {
		return 0, errors.New("DNS source response exceeds validator bound")
	}
	w.bytes += len(raw)
	return w.Hash.Write(raw)
}
