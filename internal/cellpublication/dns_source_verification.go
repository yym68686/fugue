package cellpublication

import (
	"errors"
	"sync"
	"sync/atomic"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformsafety"
)

var ErrDNSRouteSourceArtifactIntegrity = errors.New("DNS route source artifact integrity rejected")

var dnsSourceChecks dnsSourceVerificationCache

type dnsSourceVerificationCache struct {
	mu                     sync.Mutex
	entries                map[[32]byte]dnsSourceSignaturePlan
	order                  [32][32]byte
	next                   int
	hits, misses, bypasses atomic.Uint64
}

type dnsSourceSignaturePlan struct {
	proofs  []platformsafety.PreparedArtifactSignature
	current int
}

// CheckDNSRouteSourceArtifact verifies the signed DNS root plus every embedded
// publication. On an exact native-content and envelope match, reuse only
// deterministic JSON decoding, structural validation and content hashes.
// All signature checks still run against the keyring provided on this call.
// The store independently checks status, release selection, fences and LKG.
func CheckDNSRouteSourceArtifact(a model.PlatformArtifact, keys bundleauth.Keyring) error {
	return dnsSourceChecks.check(a, keys)
}

// DNSRouteSourceVerificationReuse counts proof lookup outcomes, not successful
// authorization. In particular, a hit can still fail a fresh signature check.
func DNSRouteSourceVerificationReuse() (hits, misses, bypasses uint64) {
	return dnsSourceChecks.hits.Load(), dnsSourceChecks.misses.Load(), dnsSourceChecks.bypasses.Load()
}

func (c *dnsSourceVerificationCache) check(a model.PlatformArtifact, keys bundleauth.Keyring) error {
	key, reusable := platformconfig.DNSConfigurationValidationFingerprint(a)
	if reusable {
		c.mu.Lock()
		plan, found := c.entries[key]
		c.mu.Unlock()
		if found {
			c.hits.Add(1)
			return plan.verify(keys)
		}
		c.misses.Add(1)
	} else {
		c.bypasses.Add(1)
	}
	// Bound each entry (and thus the working set) by prepared signing bytes.
	// Drop all preparation on overflow, but finish the original verification.
	var proofs []platformsafety.PreparedArtifactSignature
	bytesLeft := 128 << 10
	check := func(artifact model.PlatformArtifact) bool {
		if !platformsafety.EvaluateArtifactIntegrity(artifact, keys).Pass {
			return false
		}
		if reusable {
			proof, err := platformsafety.PrepareArtifactSignature(artifact)
			bytesLeft -= proof.Size()
			if err != nil || bytesLeft < 0 {
				reusable = false
				proofs = nil
			} else {
				proofs = append(proofs, proof)
			}
		}
		return true
	}
	if !check(a) {
		return ErrDNSRouteSourceArtifactIntegrity
	}
	pubs, err := verifyDNSArtifact(a, check)
	if err != nil || !reusable {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = make(map[[32]byte]dnsSourceSignaturePlan)
	}
	if _, found := c.entries[key]; !found {
		delete(c.entries, c.order[c.next])
		c.entries[key] = dnsSourceSignaturePlan{proofs: proofs, current: len(pubs) * 3}
		c.order[c.next] = key
		c.next = (c.next + 1) % len(c.order)
	}
	return nil
}

func (p dnsSourceSignaturePlan) verify(keys bundleauth.Keyring) error {
	for i, proof := range p.proofs {
		if err := proof.Verify(keys); err != nil {
			switch {
			case i == 0:
				return ErrDNSRouteSourceArtifactIntegrity
			case i <= p.current:
				return errors.New("referenced Cell artifact signature rejected")
			default:
				return errors.New("previous traffic artifact signature rejected")
			}
		}
	}
	return nil
}
