package platformconfig

import (
	"crypto/sha256"
	"encoding/json"
	"sync"

	"fugue/internal/model"
)

// These caches remember only successful, deterministic configuration checks.
// They store fixed-size fingerprints, never artifacts, decoded trees, plans,
// signatures, runtime facts or authorization decisions. Every lookup reads the
// actual complete content and envelope; claimed hashes are not cache identities.
// Trust, release selection, fences, expiry and live readiness remain uncached.
type dnsValidationCache struct {
	sync.Mutex
	keys map[[32]byte]bool
	ring [64][32]byte
	next int
}

var dnsPlanValidations dnsValidationCache
var dnsPreviousValidations dnsValidationCache

func (c *dnsValidationCache) contains(key [32]byte) bool {
	c.Lock()
	defer c.Unlock()
	return c.keys[key]
}
func (c *dnsValidationCache) remember(key [32]byte) {
	c.Lock()
	defer c.Unlock()
	if c.keys == nil {
		c.keys = make(map[[32]byte]bool)
	}
	if c.keys[key] {
		return
	}
	delete(c.keys, c.ring[c.next])
	c.ring[c.next] = key
	c.next = (c.next + 1) % len(c.ring)
	c.keys[key] = true
}

func dnsValidationArtifactKey(a model.PlatformArtifact) ([32]byte, int, bool) {
	contentKey, size, ok := fingerprintPolicyTree(a.Content, true)
	if !ok {
		return [32]byte{}, 0, false
	}
	a.Content = nil
	envelope, err := json.Marshal(a)
	if err != nil {
		return [32]byte{}, 0, false
	}
	h := sha256.New()
	h.Write([]byte("fugue-dns-config-validation/v1\x00"))
	h.Write(contentKey[:])
	h.Write(envelope)
	var key [32]byte
	h.Sum(key[:0])
	return key, size, true
}

func dnsPreviousValidationKey(p PreviousTrafficPublicationInput) ([32]byte, bool) {
	h := sha256.New()
	h.Write([]byte("fugue-dns-previous-validation/v1\x00"))
	size := 0
	for _, a := range []model.PlatformArtifact{p.Parent, p.Route, p.TLS, p.DNS} {
		key, n, ok := dnsValidationArtifactKey(a)
		if !ok {
			return [32]byte{}, false
		}
		size += n
		h.Write(key[:])
	}
	if size < 16<<10 {
		return [32]byte{}, false
	}
	// The remaining input is small and contains no arbitrary JSON content.
	p.Parent, p.Route, p.TLS, p.DNS = model.PlatformArtifact{}, model.PlatformArtifact{}, model.PlatformArtifact{}, model.PlatformArtifact{}
	raw, err := json.Marshal(p)
	if err != nil {
		return [32]byte{}, false
	}
	h.Write(raw)
	var key [32]byte
	h.Sum(key[:0])
	return key, true
}

// DNSConfigurationValidationFingerprint identifies complete large DNS input for
// deterministic configuration checks. It provides no trust or serving grant.
// Non-native JSON trees (including custom marshalers) are deliberately excluded
// without invoking their methods, so callers preserve the original decoder.
func DNSConfigurationValidationFingerprint(a model.PlatformArtifact) ([32]byte, bool) {
	if a.ArtifactKind != model.PlatformArtifactKindDNSAnswerBundle {
		return [32]byte{}, false
	}
	key, size, ok := dnsValidationArtifactKey(a)
	return key, ok && size >= 16<<10
}
