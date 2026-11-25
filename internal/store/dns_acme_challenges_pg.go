package store

import (
	"context"
	"fmt"
	"time"

	"fugue/internal/model"
)

func (s *Store) pgListDNSACMEChallenges(zone string, includeExpired bool) ([]model.DNSACMEChallenge, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `
SELECT id, zone, name, value, ttl, owner, created_by, expires_at, created_at, updated_at
FROM fugue_dns_acme_challenges`
	args := []any{}
	where := []string{}
	if zone != "" {
		args = append(args, zone)
		where = append(where, fmt.Sprintf("zone = $%d", len(args)))
	}
	if !includeExpired {
		args = append(args, time.Now().UTC())
		where = append(where, fmt.Sprintf("expires_at > $%d", len(args)))
	}
	for index, clause := range where {
		if index == 0 {
			query += " WHERE " + clause
		} else {
			query += " AND " + clause
		}
	}
	query += ` ORDER BY zone ASC, name ASC, expires_at ASC, id ASC`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list dns acme challenges: %w", err)
	}
	defer rows.Close()

	challenges := []model.DNSACMEChallenge{}
	for rows.Next() {
		challenge, err := scanDNSACMEChallenge(rows)
		if err != nil {
			return nil, err
		}
		challenges = append(challenges, challenge)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate dns acme challenges: %w", err)
	}
	return challenges, nil
}

func (s *Store) pgUpsertDNSACMEChallenge(challenge model.DNSACMEChallenge, actors []model.Principal) (model.DNSACMEChallenge, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	now := time.Now().UTC()
	if challenge.CreatedAt.IsZero() {
		challenge.CreatedAt = now
	}
	challenge.UpdatedAt = now
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.DNSACMEChallenge{}, err
	}
	defer tx.Rollback()
	row := tx.QueryRowContext(ctx, `
INSERT INTO fugue_dns_acme_challenges (id, zone, name, value, ttl, owner, created_by, expires_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (id) DO UPDATE SET
	zone = EXCLUDED.zone,
	name = EXCLUDED.name,
	value = EXCLUDED.value,
	ttl = EXCLUDED.ttl,
	owner = EXCLUDED.owner,
	created_by = EXCLUDED.created_by,
	expires_at = EXCLUDED.expires_at,
	updated_at = EXCLUDED.updated_at
WHERE $11 OR fugue_dns_acme_challenges.created_by=EXCLUDED.created_by
RETURNING id, zone, name, value, ttl, owner, created_by, expires_at, created_at, updated_at
`, challenge.ID, challenge.Zone, challenge.Name, challenge.Value, challenge.TTL, challenge.Owner, challenge.CreatedBy, challenge.ExpiresAt, challenge.CreatedAt, challenge.UpdatedAt, len(actors) == 0 || actors[0].IsPlatformAdmin())
	stored, err := scanDNSACMEChallenge(row)
	if err != nil {
		return model.DNSACMEChallenge{}, mapDBErr(err)
	}
	return stored, s.commitMutation(ctx, tx, mutationEvent(actors, "dns_acme_challenge.upsert", "dns_acme_challenge", stored.ID, ""))
}

func (s *Store) pgDeleteDNSACMEChallenge(id string, domains []string, actors []model.Principal) (model.DNSACMEChallenge, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.DNSACMEChallenge{}, err
	}
	defer tx.Rollback()
	creator := ""
	if len(actors) > 0 {
		creator = actors[0].ActorType + ":" + actors[0].ActorID
	}
	challenge, err := scanDNSACMEChallenge(tx.QueryRowContext(ctx, `
DELETE FROM fugue_dns_acme_challenges
WHERE id = $1 AND ($2 OR (zone=ANY($3::text[]) AND name='_acme-challenge.'||zone AND created_by=$4))
RETURNING id, zone, name, value, ttl, owner, created_by, expires_at, created_at, updated_at
`, id, len(actors) == 0 || actors[0].IsPlatformAdmin(), domains, creator))
	if err != nil {
		return model.DNSACMEChallenge{}, mapDBErr(err)
	}
	return challenge, s.commitMutation(ctx, tx, mutationEvent(actors, "dns_acme_challenge.delete", "dns_acme_challenge", challenge.ID, ""))
}

func scanDNSACMEChallenge(scanner sqlScanner) (model.DNSACMEChallenge, error) {
	var challenge model.DNSACMEChallenge
	if err := scanner.Scan(
		&challenge.ID,
		&challenge.Zone,
		&challenge.Name,
		&challenge.Value,
		&challenge.TTL,
		&challenge.Owner,
		&challenge.CreatedBy,
		&challenge.ExpiresAt,
		&challenge.CreatedAt,
		&challenge.UpdatedAt,
	); err != nil {
		return model.DNSACMEChallenge{}, err
	}
	normalizeDNSACMEChallengeForRead(&challenge)
	return challenge, nil
}
