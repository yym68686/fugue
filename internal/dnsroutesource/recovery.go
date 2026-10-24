package dnsroutesource

import (
	"crypto/sha256"
	"fmt"
	"hash"
	"slices"
	"sort"

	"fugue/internal/platformconfig"
)

// Project only the availability constraints of records/targets using a probe.
// Publication identities and unrelated policy/quality decisions are excluded;
// Build has already authenticated the complete source and applied its current
// restrictions. This digest is observation identity, never serving authority.
func recoveryConstraints(plan *platformconfig.DNSReadinessPlan) (map[string]string, error) {
	semantic := make(map[string]string, len(plan.Probes))
	for _, p := range plan.Probes {
		id := p.ID
		p.ID, p.CellPublicationDigest = "", ""
		if p.PreviousAuthority != nil {
			previous := *p.PreviousAuthority
			previous.PublicationDigest = ""
			p.PreviousAuthority = &previous
		}
		digest, err := platformconfig.Digest(p)
		if err != nil {
			return nil, err
		}
		semantic[id] = digest
	}
	// Stream ordered contributions into one hasher per probe instead of storing
	// every record/probe cross-reference. Large alias sets cannot multiply history
	// memory by the number of shared dependencies.
	hashers := map[string]hash.Hash{}
	records := slices.Clone(plan.Records)
	sort.Slice(records, func(i, j int) bool { return records[i].Hostname < records[j].Hostname })
	for _, record := range records {
		type contribution struct {
			digest string
			ids    []string
		}
		contributions := make([]contribution, 0, len(record.Targets))
		base := record
		base.Targets = nil
		for _, target := range record.Targets {
			normalized := target
			normalized.ProbeIDs = make([]string, len(target.ProbeIDs))
			for i, id := range target.ProbeIDs {
				digest, ok := semantic[id]
				if !ok {
					return nil, fmt.Errorf("recovery constraint references missing probe")
				}
				normalized.ProbeIDs[i] = digest
			}
			slices.Sort(normalized.ProbeIDs)
			digest, err := platformconfig.Digest(struct {
				Record platformconfig.DNSReadinessRecord
				Target platformconfig.DNSReadinessTarget
			}{base, normalized})
			if err != nil {
				return nil, err
			}
			contributions = append(contributions, contribution{digest, target.ProbeIDs})
		}
		sort.Slice(contributions, func(i, j int) bool { return contributions[i].digest < contributions[j].digest })
		for _, c := range contributions {
			digest := []byte(c.digest)
			for _, id := range c.ids {
				h := hashers[id]
				if h == nil {
					h = sha256.New()
					hashers[id] = h
				}
				h.Write(digest) // fixed-size SHA-256 digests frame each contribution
			}
		}
	}
	result := make(map[string]string, len(semantic))
	for id := range semantic {
		h := hashers[id]
		if h == nil {
			return nil, fmt.Errorf("recovery probe has no authorized target")
		}
		result[id] = fmt.Sprintf("sha256:%x", h.Sum(nil))
	}
	return result, nil
}
