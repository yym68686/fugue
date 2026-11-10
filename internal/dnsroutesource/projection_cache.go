package dnsroutesource

import (
	"crypto/sha256"
	"encoding/json"
	"maps"
	"slices"
	"sync"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"golang.org/x/sync/singleflight"
)

type publicationProjection struct {
	plan    *platformconfig.DNSReadinessPlan
	binding *model.TrafficReleaseBinding
	bytes   int
}

// Shared by serving/renewal/shadow compilers, bounded independently of their
// whole-selection cache. Never retains artifact bodies, mutable selections,
// LKG validity, observed clocks or positive runtime facts.
var runtimePublicationProjections publicationProjectionCache

type publicationProjectionCache struct {
	sync.Mutex
	entries                map[[32]byte]publicationProjection
	order                  [][32]byte
	bytes                  int
	hits, misses, bypasses uint64
	flights                singleflight.Group
}

func PublicationProjectionCacheObservation() map[string]uint64 {
	c := &runtimePublicationProjections
	c.Lock()
	defer c.Unlock()
	return map[string]uint64{"hits": c.hits, "misses": c.misses, "bypasses": c.bypasses, "entries": uint64(len(c.entries)), "retained_bytes": uint64(c.bytes)}
}

// Call ONLY after VerifySnapshot has checked every actual body against its
// signed content digest and authenticated all publication bindings. The digest
// is safe here because it has just been verified, never as a caller assertion.
func (c *publicationProjectionCache) buildVerified(dns model.PlatformArtifact, source platformconfig.CellDNSPlanSource, policy platformconfig.PolicySnapshot, pub model.PlatformDNSRouteSourcePublication, cell string) (*platformconfig.DNSReadinessPlan, *model.TrafficReleaseBinding, error) {
	dns.Content = nil
	keyPub := pub
	keyPub.Parent.Content, keyPub.Route.Content, keyPub.TLS.Content, keyPub.ProducerPolicy.Content = nil, nil, nil, nil
	// These affect live validity/current restrictions, not this base projection.
	keyPub.LKG, keyPub.Selections = nil, nil
	raw, err := json.Marshal(struct {
		DNS         model.PlatformArtifact
		Publication model.PlatformDNSRouteSourcePublication
		Cell        string
	}{dns, keyPub, cell})
	if err != nil || len(raw) > 64<<10 {
		c.Lock()
		c.bypasses++
		c.Unlock()
		return platformconfig.DNSRuntimeSourcePlan(source, policy, pub, cell)
	}
	key := sha256.Sum256(raw)
	value, err, _ := c.flights.Do(string(key[:]), func() (any, error) {
		c.Lock()
		entry, hit := c.entries[key]
		if hit {
			c.hits++
		} else {
			c.misses++
		}
		c.Unlock()
		if hit {
			return entry, nil
		}
		plan, binding, err := platformconfig.DNSRuntimeSourcePlan(source, policy, pub, cell)
		if err != nil {
			return nil, err
		}
		encoded, err := json.Marshal(struct {
			Plan    *platformconfig.DNSReadinessPlan
			Binding *model.TrafficReleaseBinding
		}{plan, binding})
		if err != nil {
			return nil, err
		}
		// Conservative decoded container accounting, with both bytes and entry
		// count bounded. An oversized projection is returned but never retained.
		entry = publicationProjection{plan: plan, binding: binding, bytes: 512 + 4*len(encoded)}
		c.Lock()
		defer c.Unlock()
		if entry.bytes > 8<<20 {
			c.bypasses++
			return entry, nil
		}
		if c.entries == nil {
			c.entries = map[[32]byte]publicationProjection{}
		}
		for len(c.order) > 0 && (len(c.order) >= 64 || c.bytes+entry.bytes > 8<<20) {
			old := c.order[0]
			c.order = c.order[1:]
			c.bytes -= c.entries[old].bytes
			delete(c.entries, old)
		}
		c.entries[key] = entry
		c.order = append(c.order, key)
		c.bytes += entry.bytes
		return entry, nil
	})
	if err != nil {
		return nil, nil, err
	}
	entry := value.(publicationProjection)
	binding := *entry.binding
	binding.EdgeGroupIDs = slices.Clone(binding.EdgeGroupIDs)
	return cloneReadinessPlan(entry.plan), &binding, nil
}

func cloneReadinessProbe(probe platformconfig.DNSReadinessProbe) platformconfig.DNSReadinessProbe {
	if probe.PreviousAuthority != nil {
		copy := *probe.PreviousAuthority
		probe.PreviousAuthority = &copy
	}
	if probe.StaticEdge != nil {
		copy := *probe.StaticEdge
		probe.StaticEdge = &copy
	}
	return probe
}

func cloneReadinessPlan(plan *platformconfig.DNSReadinessPlan) *platformconfig.DNSReadinessPlan {
	out := *plan
	out.Probes = slices.Clone(plan.Probes)
	for i := range out.Probes {
		out.Probes[i] = cloneReadinessProbe(out.Probes[i])
	}
	out.Records = slices.Clone(plan.Records)
	for i := range out.Records {
		r := &out.Records[i]
		r.MinDistinctDomains = maps.Clone(r.MinDistinctDomains)
		r.Targets = slices.Clone(r.Targets)
		for j := range r.Targets {
			r.Targets[j].FailureDomains = maps.Clone(r.Targets[j].FailureDomains)
			r.Targets[j].ProbeIDs = slices.Clone(r.Targets[j].ProbeIDs)
		}
	}
	return &out
}
