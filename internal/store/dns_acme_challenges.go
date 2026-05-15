package store

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

const (
	defaultDNSACMEChallengeTTL = 60
	maxDNSACMEChallengeTTL     = 3600
)

func (s *Store) ListDNSACMEChallenges(zone string, includeExpired bool) ([]model.DNSACMEChallenge, error) {
	zone = normalizeDNSZone(zone)
	if s.usingDatabase() {
		return s.pgListDNSACMEChallenges(zone, includeExpired)
	}

	now := time.Now().UTC()
	var challenges []model.DNSACMEChallenge
	err := s.withLockedState(false, func(state *model.State) error {
		for _, challenge := range state.DNSACMEChallenges {
			normalizeDNSACMEChallengeForRead(&challenge)
			if zone != "" && !strings.EqualFold(challenge.Zone, zone) {
				continue
			}
			if !includeExpired && !challenge.ExpiresAt.After(now) {
				continue
			}
			challenges = append(challenges, challenge)
		}
		sortDNSACMEChallenges(challenges)
		return nil
	})
	return challenges, err
}

func (s *Store) UpsertDNSACMEChallenge(challenge model.DNSACMEChallenge) (model.DNSACMEChallenge, error) {
	challenge, err := normalizeDNSACMEChallengeForStore(challenge)
	if err != nil {
		return model.DNSACMEChallenge{}, err
	}
	if s.usingDatabase() {
		return s.pgUpsertDNSACMEChallenge(challenge)
	}

	var out model.DNSACMEChallenge
	err = s.withLockedState(true, func(state *model.State) error {
		now := time.Now().UTC()
		index := findDNSACMEChallenge(state, challenge.ID)
		if index >= 0 {
			existing := state.DNSACMEChallenges[index]
			if challenge.CreatedAt.IsZero() {
				challenge.CreatedAt = existing.CreatedAt
			}
		} else if challenge.CreatedAt.IsZero() {
			challenge.CreatedAt = now
		}
		challenge.UpdatedAt = now
		if index >= 0 {
			state.DNSACMEChallenges[index] = challenge
		} else {
			state.DNSACMEChallenges = append(state.DNSACMEChallenges, challenge)
		}
		out = challenge
		return nil
	})
	if err != nil {
		return model.DNSACMEChallenge{}, err
	}
	normalizeDNSACMEChallengeForRead(&out)
	return out, nil
}

func (s *Store) DeleteDNSACMEChallenge(id string) (model.DNSACMEChallenge, error) {
	id = normalizeDNSACMEChallengeID(id)
	if id == "" {
		return model.DNSACMEChallenge{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgDeleteDNSACMEChallenge(id)
	}

	var removed model.DNSACMEChallenge
	err := s.withLockedState(true, func(state *model.State) error {
		index := findDNSACMEChallenge(state, id)
		if index < 0 {
			return ErrNotFound
		}
		removed = state.DNSACMEChallenges[index]
		state.DNSACMEChallenges = append(state.DNSACMEChallenges[:index], state.DNSACMEChallenges[index+1:]...)
		return nil
	})
	if err != nil {
		return model.DNSACMEChallenge{}, err
	}
	normalizeDNSACMEChallengeForRead(&removed)
	return removed, nil
}

func normalizeDNSACMEChallengeForStore(challenge model.DNSACMEChallenge) (model.DNSACMEChallenge, error) {
	challenge.Zone = normalizeDNSZone(challenge.Zone)
	challenge.Name = normalizeDNSZone(challenge.Name)
	challenge.Value = strings.TrimSpace(challenge.Value)
	challenge.Owner = strings.TrimSpace(challenge.Owner)
	challenge.CreatedBy = strings.TrimSpace(challenge.CreatedBy)
	if challenge.Zone == "" && strings.HasPrefix(challenge.Name, "_acme-challenge.") {
		challenge.Zone = strings.TrimPrefix(challenge.Name, "_acme-challenge.")
	}
	if challenge.Zone == "" || challenge.Name == "" || challenge.Value == "" {
		return model.DNSACMEChallenge{}, ErrInvalidInput
	}
	if !strings.HasPrefix(challenge.Name, "_acme-challenge.") || !edgeDNSTargetWithinStoreZone(challenge.Name, challenge.Zone) {
		return model.DNSACMEChallenge{}, ErrInvalidInput
	}
	if challenge.TTL <= 0 {
		challenge.TTL = defaultDNSACMEChallengeTTL
	}
	if challenge.TTL > maxDNSACMEChallengeTTL {
		return model.DNSACMEChallenge{}, ErrInvalidInput
	}
	if !challenge.ExpiresAt.After(time.Now().UTC()) {
		return model.DNSACMEChallenge{}, ErrInvalidInput
	}
	if challenge.ID == "" {
		challenge.ID = dnsACMEChallengeID(challenge.Zone, challenge.Name, challenge.Value)
	} else {
		challenge.ID = normalizeDNSACMEChallengeID(challenge.ID)
	}
	if challenge.ID == "" {
		return model.DNSACMEChallenge{}, ErrInvalidInput
	}
	return challenge, nil
}

func normalizeDNSACMEChallengeForRead(challenge *model.DNSACMEChallenge) {
	if challenge == nil {
		return
	}
	challenge.ID = normalizeDNSACMEChallengeID(challenge.ID)
	challenge.Zone = normalizeDNSZone(challenge.Zone)
	challenge.Name = normalizeDNSZone(challenge.Name)
	challenge.Value = strings.TrimSpace(challenge.Value)
	challenge.Owner = strings.TrimSpace(challenge.Owner)
	challenge.CreatedBy = strings.TrimSpace(challenge.CreatedBy)
	if challenge.TTL <= 0 {
		challenge.TTL = defaultDNSACMEChallengeTTL
	}
}

func findDNSACMEChallenge(state *model.State, id string) int {
	if state == nil {
		return -1
	}
	id = normalizeDNSACMEChallengeID(id)
	for index, challenge := range state.DNSACMEChallenges {
		if strings.EqualFold(challenge.ID, id) {
			return index
		}
	}
	return -1
}

func sortDNSACMEChallenges(challenges []model.DNSACMEChallenge) {
	sort.Slice(challenges, func(i, j int) bool {
		if challenges[i].Zone != challenges[j].Zone {
			return challenges[i].Zone < challenges[j].Zone
		}
		if challenges[i].Name != challenges[j].Name {
			return challenges[i].Name < challenges[j].Name
		}
		if !challenges[i].ExpiresAt.Equal(challenges[j].ExpiresAt) {
			return challenges[i].ExpiresAt.Before(challenges[j].ExpiresAt)
		}
		return challenges[i].ID < challenges[j].ID
	})
}

func dnsACMEChallengeID(zone, name, value string) string {
	material := normalizeDNSZone(zone) + "\x00" + normalizeDNSZone(name) + "\x00" + strings.TrimSpace(value)
	sum := sha256.Sum256([]byte(material))
	return "dnsacme_" + hex.EncodeToString(sum[:])[:20]
}

func normalizeDNSACMEChallengeID(id string) string {
	return strings.TrimSpace(strings.ToLower(id))
}

func edgeDNSTargetWithinStoreZone(target, zone string) bool {
	target = normalizeDNSZone(target)
	zone = normalizeDNSZone(zone)
	return target != "" && zone != "" && (target == zone || strings.HasSuffix(target, "."+zone))
}
