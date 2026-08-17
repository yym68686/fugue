package store

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/trafficoverride"

	k8svalidation "k8s.io/apimachinery/pkg/util/validation"
)

const trafficOverrideMaxLifetime = 24 * time.Hour

func (s *Store) ListTrafficOverrides() ([]model.TrafficOverride, error) {
	if s.usingDatabase() {
		return s.pgListTrafficOverrides()
	}
	var out []model.TrafficOverride
	err := s.withLockedState(false, func(state *model.State) error {
		out = cloneTrafficOverrides(state.TrafficOverrides)
		sort.Slice(out, func(i, j int) bool { return out[i].Hostname < out[j].Hostname })
		return nil
	})
	return out, err
}

func (s *Store) GetTrafficOverride(hostname string) (model.TrafficOverride, error) {
	hostname, err := normalizeTrafficOverrideHostname(hostname)
	if err != nil {
		return model.TrafficOverride{}, err
	}
	if s.usingDatabase() {
		return s.pgGetTrafficOverride(hostname)
	}
	var out model.TrafficOverride
	err = s.withLockedState(false, func(state *model.State) error {
		for _, override := range state.TrafficOverrides {
			if override.Hostname == hostname {
				out = cloneTrafficOverride(override)
				return nil
			}
		}
		return ErrNotFound
	})
	return out, err
}

func (s *Store) PutTrafficOverrideCAS(candidate model.TrafficOverride, expectedGeneration uint64) (model.TrafficOverride, error) {
	candidate, err := normalizeTrafficOverride(candidate, time.Now().UTC())
	if err != nil {
		return model.TrafficOverride{}, err
	}
	if candidate.Generation != expectedGeneration+1 {
		return model.TrafficOverride{}, ErrInvalidInput
	}
	keyring, err := s.GetTrafficOverrideSigningKeyring()
	if err != nil {
		return model.TrafficOverride{}, err
	}
	if err := trafficoverride.VerifyWithKeyring(candidate, keyring); err != nil {
		return model.TrafficOverride{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgPutTrafficOverrideCAS(candidate, expectedGeneration)
	}
	var out model.TrafficOverride
	err = s.withLockedState(true, func(state *model.State) error {
		index := -1
		for i := range state.TrafficOverrides {
			if state.TrafficOverrides[i].Hostname == candidate.Hostname {
				index = i
				break
			}
		}
		if index < 0 {
			if expectedGeneration != 0 {
				return ErrConflict
			}
			state.TrafficOverrides = append(state.TrafficOverrides, candidate)
		} else {
			current := state.TrafficOverrides[index]
			if current.Generation != expectedGeneration {
				return ErrConflict
			}
			candidate.CreatedAt = current.CreatedAt
			state.TrafficOverrides[index] = candidate
		}
		out = cloneTrafficOverride(candidate)
		return nil
	})
	return out, err
}

func (s *Store) GetTrafficOverrideSigningKeyring() (model.TrafficOverrideSigningKeyring, error) {
	if s.usingDatabase() {
		keyring, err := s.pgGetTrafficOverrideSigningKeyring()
		if err == nil || !errors.Is(err, ErrNotFound) {
			return keyring, err
		}
		if err := s.pgEnsureTrafficOverrideSigningKeyring(); err != nil {
			return model.TrafficOverrideSigningKeyring{}, err
		}
		return s.pgGetTrafficOverrideSigningKeyring()
	}
	var out model.TrafficOverrideSigningKeyring
	err := s.withLockedState(true, func(state *model.State) error {
		if state.TrafficOverrideSigning == nil {
			keyring, err := newTrafficOverrideSigningKeyring(time.Now().UTC())
			if err != nil {
				return err
			}
			state.TrafficOverrideSigning = &keyring
		}
		out = *state.TrafficOverrideSigning
		return nil
	})
	return out, err
}

func (s *Store) RotateTrafficOverrideSigningKeyring(expectedGeneration uint64) (model.TrafficOverrideSigningKeyring, error) {
	if s.usingDatabase() {
		return s.pgRotateTrafficOverrideSigningKeyring(expectedGeneration)
	}
	var out model.TrafficOverrideSigningKeyring
	err := s.withLockedState(true, func(state *model.State) error {
		if state.TrafficOverrideSigning == nil || state.TrafficOverrideSigning.Generation != expectedGeneration {
			return ErrConflict
		}
		now := time.Now().UTC()
		next, err := rotateTrafficOverrideSigningKeyring(*state.TrafficOverrideSigning, now)
		if err != nil {
			return err
		}
		state.TrafficOverrideSigning = &next
		out = next
		return nil
	})
	return out, err
}

func normalizeTrafficOverride(candidate model.TrafficOverride, now time.Time) (model.TrafficOverride, error) {
	var err error
	candidate.Hostname, err = normalizeTrafficOverrideHostname(candidate.Hostname)
	if err != nil {
		return model.TrafficOverride{}, err
	}
	candidate.Schema = model.TrafficOverrideSchemaV1
	candidate.State = strings.TrimSpace(strings.ToLower(candidate.State))
	if candidate.State != model.TrafficOverrideStateStaged && candidate.State != model.TrafficOverrideStateRevoked {
		return model.TrafficOverride{}, ErrInvalidInput
	}
	candidate.Answers = uniqueSortedStoreStrings(candidate.Answers)
	if len(candidate.Answers) == 0 || len(candidate.Answers) > model.TrafficOverrideMaxAnswers {
		return model.TrafficOverride{}, ErrInvalidInput
	}
	for _, answer := range candidate.Answers {
		if net.ParseIP(answer) == nil {
			return model.TrafficOverride{}, ErrInvalidInput
		}
	}
	routes := make([]string, 0, len(candidate.RequiredHostRoutes))
	for _, route := range candidate.RequiredHostRoutes {
		route, err = normalizeTrafficOverrideHostname(route)
		if err != nil {
			return model.TrafficOverride{}, err
		}
		routes = append(routes, route)
	}
	candidate.RequiredHostRoutes = uniqueSortedStoreStrings(routes)
	if len(candidate.RequiredHostRoutes) == 0 || len(candidate.RequiredHostRoutes) > model.TrafficOverrideMaxRequiredRoutes || !stringSliceContainsStore(candidate.RequiredHostRoutes, candidate.Hostname) {
		return model.TrafficOverride{}, ErrInvalidInput
	}
	candidate.RouteGeneration = strings.TrimSpace(candidate.RouteGeneration)
	candidate.RouteDigest = strings.TrimSpace(strings.ToLower(candidate.RouteDigest))
	if candidate.RouteGeneration == "" || !validTrafficOverrideDigest(candidate.RouteDigest) {
		return model.TrafficOverride{}, ErrInvalidInput
	}
	candidate.Reason = strings.TrimSpace(candidate.Reason)
	candidate.Operator = strings.TrimSpace(candidate.Operator)
	candidate.ArtifactDigest = strings.TrimSpace(strings.ToLower(candidate.ArtifactDigest))
	candidate.KeyID = strings.TrimSpace(candidate.KeyID)
	candidate.Signature = strings.TrimSpace(candidate.Signature)
	if len(candidate.Reason) < 8 || candidate.Operator == "" || !validTrafficOverrideDigest(candidate.ArtifactDigest) || candidate.KeyID == "" || !strings.HasPrefix(candidate.Signature, "ed25519:") {
		return model.TrafficOverride{}, ErrInvalidInput
	}
	now = now.UTC()
	candidate.ExpiresAt = candidate.ExpiresAt.UTC()
	candidate.SignedAt = candidate.SignedAt.UTC()
	candidate.CreatedAt = candidate.CreatedAt.UTC()
	candidate.UpdatedAt = candidate.UpdatedAt.UTC()
	if candidate.ExpiresAt.IsZero() || !candidate.ExpiresAt.After(now) || candidate.ExpiresAt.After(now.Add(trafficOverrideMaxLifetime)) || candidate.SignedAt.IsZero() || candidate.CreatedAt.IsZero() || candidate.UpdatedAt.IsZero() {
		return model.TrafficOverride{}, ErrInvalidInput
	}
	return candidate, nil
}

func normalizeTrafficOverrideHostname(raw string) (string, error) {
	hostname := strings.Trim(strings.TrimSpace(strings.ToLower(raw)), ".")
	if hostname == "" || len(hostname) > 253 || len(k8svalidation.IsDNS1123Subdomain(hostname)) > 0 {
		return "", ErrInvalidInput
	}
	return hostname, nil
}

func validTrafficOverrideDigest(value string) bool {
	if !strings.HasPrefix(value, "sha256:") || len(value) != len("sha256:")+64 {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil
}

func stringSliceContainsStore(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func cloneTrafficOverride(override model.TrafficOverride) model.TrafficOverride {
	out := override
	out.Answers = append([]string{}, override.Answers...)
	out.RequiredHostRoutes = append([]string{}, override.RequiredHostRoutes...)
	return out
}

func cloneTrafficOverrides(values []model.TrafficOverride) []model.TrafficOverride {
	out := make([]model.TrafficOverride, 0, len(values))
	for _, value := range values {
		out = append(out, cloneTrafficOverride(value))
	}
	return out
}

func newTrafficOverrideSigningKeyring(now time.Time) (model.TrafficOverrideSigningKeyring, error) {
	privateKey, publicKey, keyID, err := generateTrafficOverrideSigningKey()
	if err != nil {
		return model.TrafficOverrideSigningKeyring{}, err
	}
	now = now.UTC()
	return model.TrafficOverrideSigningKeyring{Schema: model.TrafficOverrideSigningSchemaV1, Generation: 1, CurrentKeyID: keyID, CurrentPrivateKey: privateKey, CurrentPublicKey: publicKey, CreatedAt: now, UpdatedAt: now}, nil
}

func rotateTrafficOverrideSigningKeyring(current model.TrafficOverrideSigningKeyring, now time.Time) (model.TrafficOverrideSigningKeyring, error) {
	privateKey, publicKey, keyID, err := generateTrafficOverrideSigningKey()
	if err != nil {
		return model.TrafficOverrideSigningKeyring{}, err
	}
	now = now.UTC()
	current.Schema = model.TrafficOverrideSigningSchemaV1
	current.Generation++
	current.PreviousKeyID = current.CurrentKeyID
	current.PreviousPrivateKey = current.CurrentPrivateKey
	current.PreviousPublicKey = current.CurrentPublicKey
	current.CurrentKeyID = keyID
	current.CurrentPrivateKey = privateKey
	current.CurrentPublicKey = publicKey
	current.RotatedAt = now
	current.UpdatedAt = now
	return current, nil
}

func generateTrafficOverrideSigningKey() (string, string, string, error) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return "", "", "", fmt.Errorf("generate traffic override signing key: %w", err)
	}
	digest := sha256.Sum256(publicKey)
	return base64.RawStdEncoding.EncodeToString(privateKey), base64.RawStdEncoding.EncodeToString(publicKey), "traffic-override-" + hex.EncodeToString(digest[:8]), nil
}

func (s *Store) pgEnsureTrafficOverrideSigningKeyring() error {
	if _, err := s.pgGetTrafficOverrideSigningKeyring(); err == nil {
		return nil
	} else if !errors.Is(err, ErrNotFound) {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, "traffic-override-signing-key"); err != nil {
		return err
	}
	var exists bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM fugue_traffic_override_signing_keys WHERE singleton=true)`).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		keyring, err := newTrafficOverrideSigningKeyring(time.Now().UTC())
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO fugue_traffic_override_signing_keys (singleton,schema,generation,current_key_id,current_private_key,current_public_key,previous_key_id,previous_private_key,previous_public_key,created_at,rotated_at,updated_at) VALUES (true,$1,$2,$3,$4,$5,'','','',$6,NULL,$6)`, keyring.Schema, keyring.Generation, keyring.CurrentKeyID, keyring.CurrentPrivateKey, keyring.CurrentPublicKey, keyring.CreatedAt); err != nil {
			return mapDBErr(err)
		}
	}
	return tx.Commit()
}

func (s *Store) pgGetTrafficOverrideSigningKeyring() (model.TrafficOverrideSigningKeyring, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return scanTrafficOverrideSigningKeyring(s.db.QueryRowContext(ctx, `SELECT schema,generation,current_key_id,current_private_key,current_public_key,previous_key_id,previous_private_key,previous_public_key,created_at,rotated_at,updated_at FROM fugue_traffic_override_signing_keys WHERE singleton=true`))
}

func (s *Store) pgRotateTrafficOverrideSigningKeyring(expectedGeneration uint64) (model.TrafficOverrideSigningKeyring, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.TrafficOverrideSigningKeyring{}, err
	}
	defer tx.Rollback()
	current, err := scanTrafficOverrideSigningKeyring(tx.QueryRowContext(ctx, `SELECT schema,generation,current_key_id,current_private_key,current_public_key,previous_key_id,previous_private_key,previous_public_key,created_at,rotated_at,updated_at FROM fugue_traffic_override_signing_keys WHERE singleton=true FOR UPDATE`))
	if err != nil {
		return model.TrafficOverrideSigningKeyring{}, err
	}
	if current.Generation != expectedGeneration {
		return model.TrafficOverrideSigningKeyring{}, ErrConflict
	}
	next, err := rotateTrafficOverrideSigningKeyring(current, time.Now().UTC())
	if err != nil {
		return model.TrafficOverrideSigningKeyring{}, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE fugue_traffic_override_signing_keys SET schema=$1,generation=$2,current_key_id=$3,current_private_key=$4,current_public_key=$5,previous_key_id=$6,previous_private_key=$7,previous_public_key=$8,rotated_at=$9,updated_at=$9 WHERE singleton=true AND generation=$10`, next.Schema, next.Generation, next.CurrentKeyID, next.CurrentPrivateKey, next.CurrentPublicKey, next.PreviousKeyID, next.PreviousPrivateKey, next.PreviousPublicKey, next.RotatedAt, expectedGeneration)
	if err != nil {
		return model.TrafficOverrideSigningKeyring{}, mapDBErr(err)
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return model.TrafficOverrideSigningKeyring{}, ErrConflict
	}
	if err := tx.Commit(); err != nil {
		return model.TrafficOverrideSigningKeyring{}, err
	}
	return next, nil
}

func scanTrafficOverrideSigningKeyring(row interface{ Scan(...any) error }) (model.TrafficOverrideSigningKeyring, error) {
	var keyring model.TrafficOverrideSigningKeyring
	var rotatedAt sql.NullTime
	if err := row.Scan(&keyring.Schema, &keyring.Generation, &keyring.CurrentKeyID, &keyring.CurrentPrivateKey, &keyring.CurrentPublicKey, &keyring.PreviousKeyID, &keyring.PreviousPrivateKey, &keyring.PreviousPublicKey, &keyring.CreatedAt, &rotatedAt, &keyring.UpdatedAt); err != nil {
		return model.TrafficOverrideSigningKeyring{}, mapDBErr(err)
	}
	if rotatedAt.Valid {
		keyring.RotatedAt = rotatedAt.Time
	}
	return keyring, nil
}

func (s *Store) pgListTrafficOverrides() ([]model.TrafficOverride, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, `SELECT artifact_json FROM fugue_traffic_overrides ORDER BY hostname`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.TrafficOverride{}
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var override model.TrafficOverride
		if err := json.Unmarshal(raw, &override); err != nil {
			return nil, err
		}
		out = append(out, override)
	}
	return out, rows.Err()
}

func (s *Store) pgGetTrafficOverride(hostname string) (model.TrafficOverride, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var raw []byte
	if err := s.db.QueryRowContext(ctx, `SELECT artifact_json FROM fugue_traffic_overrides WHERE hostname=$1`, hostname).Scan(&raw); err != nil {
		return model.TrafficOverride{}, mapDBErr(err)
	}
	var override model.TrafficOverride
	if err := json.Unmarshal(raw, &override); err != nil {
		return model.TrafficOverride{}, err
	}
	return override, nil
}

func (s *Store) pgPutTrafficOverrideCAS(candidate model.TrafficOverride, expectedGeneration uint64) (model.TrafficOverride, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.TrafficOverride{}, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, "traffic-override:"+candidate.Hostname); err != nil {
		return model.TrafficOverride{}, err
	}
	var currentGeneration uint64
	var createdAt time.Time
	err = tx.QueryRowContext(ctx, `SELECT generation,created_at FROM fugue_traffic_overrides WHERE hostname=$1 FOR UPDATE`, candidate.Hostname).Scan(&currentGeneration, &createdAt)
	switch {
	case err == sql.ErrNoRows:
		if expectedGeneration != 0 {
			return model.TrafficOverride{}, ErrConflict
		}
	case err != nil:
		return model.TrafficOverride{}, mapDBErr(err)
	default:
		if currentGeneration != expectedGeneration {
			return model.TrafficOverride{}, ErrConflict
		}
		candidate.CreatedAt = createdAt
	}
	raw, err := json.Marshal(candidate)
	if err != nil {
		return model.TrafficOverride{}, err
	}
	if expectedGeneration == 0 {
		_, err = tx.ExecContext(ctx, `INSERT INTO fugue_traffic_overrides (hostname,generation,state,artifact_json,expires_at,created_at,updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7)`, candidate.Hostname, candidate.Generation, candidate.State, raw, candidate.ExpiresAt, candidate.CreatedAt, candidate.UpdatedAt)
	} else {
		var result sql.Result
		result, err = tx.ExecContext(ctx, `UPDATE fugue_traffic_overrides SET generation=$2,state=$3,artifact_json=$4,expires_at=$5,updated_at=$6 WHERE hostname=$1 AND generation=$7`, candidate.Hostname, candidate.Generation, candidate.State, raw, candidate.ExpiresAt, candidate.UpdatedAt, expectedGeneration)
		if err == nil {
			if rows, _ := result.RowsAffected(); rows != 1 {
				return model.TrafficOverride{}, ErrConflict
			}
		}
	}
	if err != nil {
		return model.TrafficOverride{}, mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return model.TrafficOverride{}, err
	}
	return candidate, nil
}
