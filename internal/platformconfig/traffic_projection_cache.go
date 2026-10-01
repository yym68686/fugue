package platformconfig

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"sync"

	"fugue/internal/model"
)

// Only the pure policy/topology/cohort projection is memoized. Artifact
// integrity, active release, trust, fencing and membership revision checks
// remain the caller's responsibility on every request, as before.
var trafficProjectionValidations = struct {
	sync.Mutex
	keys  map[[32]byte]bool
	ring  [256][32]byte
	next  int
	count int
}{keys: make(map[[32]byte]bool)}

func trafficProjectionKey(parent, child model.PlatformArtifact, policy []byte) ([32]byte, bool) {
	if len(policy) < 2048 {
		return [32]byte{}, false
	}
	// The validator reads the child's policy and its envelope, plus the
	// parent's complete content and envelope. Include all of those actual
	// bytes, never just claimed digests. If new child content fields are used
	// by validateTrafficCohortPolicy, they must also be included here.
	child.Content = nil
	parentRaw, err := json.Marshal(parent)
	if err != nil {
		return [32]byte{}, false
	}
	childRaw, err := json.Marshal(child)
	if err != nil {
		return [32]byte{}, false
	}
	h := sha256.New()
	_, _ = h.Write([]byte("fugue-traffic-projection/v1\x00"))
	var length [8]byte
	for _, raw := range [][]byte{parentRaw, childRaw, policy} {
		binary.LittleEndian.PutUint64(length[:], uint64(len(raw)))
		_, _ = h.Write(length[:])
		_, _ = h.Write(raw)
	}
	var key [32]byte
	h.Sum(key[:0])
	return key, true
}

func trafficProjectionCached(key [32]byte) bool {
	c := &trafficProjectionValidations
	c.Lock()
	defer c.Unlock()
	return c.keys[key]
}

func cacheTrafficProjection(key [32]byte) {
	c := &trafficProjectionValidations
	c.Lock()
	defer c.Unlock()
	if c.keys[key] {
		return
	}
	if c.count == len(c.ring) {
		delete(c.keys, c.ring[c.next])
	} else {
		c.count++
	}
	c.ring[c.next] = key
	c.next = (c.next + 1) % len(c.ring)
	c.keys[key] = true
}
