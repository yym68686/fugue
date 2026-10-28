package dnsroutesource

import (
	"maps"
	"slices"
	"sync"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

// Compiler retains one deterministic projection, not an authorization decision
// or positive runtime fact. Every call verifies the complete fresh snapshot,
// signatures, key revocations and LKG deadlines before consulting the cache.
// Its key is used only after verification recomputes the selection digest from
// actual snapshot content/envelopes and checks the DNS content hash and binding.
type Compiler struct {
	mu    sync.Mutex
	key   string
	plans *Plans
}

func (c *Compiler) Build(dns model.PlatformArtifact, snapshot model.PlatformDNSRouteSourceSnapshot, keys bundleauth.Keyring, now time.Time) (*Plans, error) {
	if err := VerifySnapshot(snapshot, dns, keys, now); err != nil {
		return nil, err
	}
	// VerifySnapshot has just authenticated dns.Content against ContentHash
	// and recomputed SelectionDigest, which includes DNSArtifactID/Digest and
	// every source publication and lane (but not the observation timestamp).
	// Re-serializing the same full tree here adds no integrity protection.
	key := snapshot.SelectionDigest
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.plans == nil || c.key != key {
		plans, err := buildVerified(dns, snapshot)
		if err != nil {
			return nil, err
		}
		// Do not keep another full publication/artifact tree alive in the cache.
		plans.Snapshot = model.PlatformDNSRouteSourceSnapshot{}
		c.key, c.plans = key, plans
	}
	result := clonePlans(c.plans)
	result.Snapshot = snapshot
	return result, nil
}

// Callers own their projections. Neither mutations nor concurrent observation
// rounds can contaminate a later cache hit.
func clonePlans(p *Plans) *Plans {
	out := *p
	out.RecoveryConstraints = maps.Clone(p.RecoveryConstraints)
	out.Probes = maps.Clone(p.Probes)
	for id, probe := range out.Probes {
		if probe.PreviousAuthority != nil {
			previous := *probe.PreviousAuthority
			probe.PreviousAuthority = &previous
			out.Probes[id] = probe
		}
	}
	out.Bindings = make(map[string]*model.TrafficReleaseBinding, len(p.Bindings))
	for id, binding := range p.Bindings {
		copy := *binding
		copy.EdgeGroupIDs = slices.Clone(binding.EdgeGroupIDs)
		out.Bindings[id] = &copy
	}
	cloneTarget := func(t platformconfig.DNSReadinessTarget) platformconfig.DNSReadinessTarget {
		t.FailureDomains = maps.Clone(t.FailureDomains)
		t.ProbeIDs = slices.Clone(t.ProbeIDs)
		return t
	}
	out.records = slices.Clone(p.records)
	for i := range out.records {
		r := &out.records[i]
		r.MinDistinctDomains = maps.Clone(r.MinDistinctDomains)
		r.Targets = slices.Clone(r.Targets)
		for j := range r.Targets {
			r.Targets[j] = cloneTarget(r.Targets[j])
		}
	}
	out.targets = make(map[string][]variant, len(p.targets))
	for key, variants := range p.targets {
		copies := slices.Clone(variants)
		for i := range copies {
			copies[i].target = cloneTarget(copies[i].target)
		}
		out.targets[key] = copies
	}
	out.targetKeys = make(map[string][]string, len(p.targetKeys))
	for key, values := range p.targetKeys {
		out.targetKeys[key] = slices.Clone(values)
	}
	return &out
}
