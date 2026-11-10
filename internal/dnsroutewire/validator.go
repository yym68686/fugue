// Package dnsroutewire defines the byte-bound validator of a source observation.
package dnsroutewire

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"slices"
	"strconv"
	"sync"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformsafety"
)

const ObservedAtHeader = "X-Fugue-Observed-At"

var sourceValidators struct {
	sync.Mutex
	values map[[32]byte]string
	order  [128][32]byte
	next   int
}

// Validator includes all actual content and authority envelopes. Only the
// observation time is excluded; it must be supplied freshly on every recheck.
// It is a weak HTTP validator because that one response field changes.
func Validator(response model.PlatformConsumerDNSRouteSourcesResponse, retained *model.PlatformLKGSnapshot) (string, error) {
	response.Snapshot.ObservedAt = time.Time{}
	value := model.PlatformConsumerRetainedDNSRouteSourcesResponse{PlatformConsumerDNSRouteSourcesResponse: response}
	if retained != nil {
		value.RetainedLKG = *retained
	}
	key, reusable := sourceValidatorKey(value)
	if reusable {
		sourceValidators.Lock()
		prior, hit := sourceValidators.values[key]
		sourceValidators.Unlock()
		if hit {
			return prior, nil
		}
	}
	etag, err := validatorUncached(value)
	if err != nil || !reusable {
		return etag, err
	}
	sourceValidators.Lock()
	defer sourceValidators.Unlock()
	if sourceValidators.values == nil {
		sourceValidators.values = map[[32]byte]string{}
	}
	if _, exists := sourceValidators.values[key]; !exists {
		delete(sourceValidators.values, sourceValidators.order[sourceValidators.next])
		sourceValidators.order[sourceValidators.next] = key
		sourceValidators.next = (sourceValidators.next + 1) % len(sourceValidators.order)
		sourceValidators.values[key] = etag
	}
	return etag, nil
}

// Only a successful bounded v1 encoding is remembered. Native content trees
// are fingerprinted afresh on EVERY call; declared hashes are never validators.
// The complete fresh envelope remains in the key. Unsupported/custom trees
// retain the exact ordinary JSON path. No response body or authority is cached.
func sourceValidatorKey(value model.PlatformConsumerRetainedDNSRouteSourcesResponse) ([32]byte, bool) {
	h := sha256.New()
	_, _ = h.Write([]byte("fugue-dns-source-validator-reuse/v1\x00"))
	value.Snapshot.Scopes = slices.Clone(value.Snapshot.Scopes)
	for i := range value.Snapshot.Scopes {
		scope := &value.Snapshot.Scopes[i]
		scope.Publications = slices.Clone(scope.Publications)
		for j := range scope.Publications {
			pub := &scope.Publications[j]
			for _, artifact := range []*model.PlatformArtifact{&pub.Parent, &pub.Route, &pub.TLS, &pub.ProducerPolicy} {
				fingerprint, ok := platformsafety.FingerprintArtifactContent(artifact.Content)
				if !ok {
					return [32]byte{}, false
				}
				_, _ = h.Write(fingerprint[:])
				artifact.Content = nil
			}
		}
	}
	raw, err := json.Marshal(value)
	if err != nil || len(raw) > 1<<20 {
		return [32]byte{}, false
	}
	_, _ = h.Write(raw)
	var key [32]byte
	h.Sum(key[:0])
	return key, true
}

func validatorUncached(value model.PlatformConsumerRetainedDNSRouteSourcesResponse) (string, error) {
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
