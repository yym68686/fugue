package platformproducer

import (
	"crypto/sha256"
	"encoding/json"
	"sync"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

// Cache only deterministic output/configuration equivalence, never signature
// trust, producer activation, current lanes, LKG expiry or runtime readiness.
// Every lookup fingerprints the actual complete input and policy; claimed
// digests are not cache identities. Fixed fingerprints retain no artifact tree.
type dnsOutputValidationCache struct {
	sync.Mutex
	keys map[[32]byte]bool
	ring [64][32]byte
	next int
}

var dnsOutputValidations dnsOutputValidationCache

func dnsOutputValidationKey(policy Policy, artifact model.PlatformArtifact) ([32]byte, bool) {
	content, ok := platformconfig.DNSConfigurationValidationFingerprint(artifact)
	if !ok {
		return [32]byte{}, false
	}
	raw, err := json.Marshal(policy)
	if err != nil {
		return [32]byte{}, false
	}
	h := sha256.New()
	h.Write([]byte("fugue-dns-producer-output-validation/v1\x00"))
	h.Write(content[:])
	h.Write(raw)
	var key [32]byte
	h.Sum(key[:0])
	return key, true
}
func (c *dnsOutputValidationCache) contains(key [32]byte) bool {
	c.Lock()
	defer c.Unlock()
	return c.keys[key]
}
func (c *dnsOutputValidationCache) remember(key [32]byte) {
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
