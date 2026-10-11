package store

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sort"
	"strings"
	"syscall"
	"time"

	"fugue/internal/model"
)

func (stateStore *Store) RecordAppSSHObservation(observation model.AppSSHObservation) error {
	identity, err := stateStore.GetAppSSHIdentity(observation.TenantID, observation.AppID)
	if err != nil {
		return err
	}
	endpoint, err := stateStore.GetAppSSHEndpoint(observation.AppID)
	if err != nil {
		return err
	}
	if identity.ID != observation.IdentityID || endpoint.ID != observation.EndpointID || model.AppSSHRouteDigest(endpoint) != observation.RouteDigest || model.AppSSHPublicAddress(endpoint) != observation.Target {
		return ErrConflict
	}
	if err := validateSSHObservation(observation, identity, time.Now().UTC()); err != nil {
		return err
	}
	if stateStore.usingDatabase() {
		raw, err := json.Marshal(observation)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err = stateStore.db.ExecContext(ctx, `INSERT INTO fugue_app_ssh_observations(id,tenant_id,app_id,identity_id,observed_at,body) VALUES($1,$2,$3,$4,$5,$6::jsonb)`, observation.ID, observation.TenantID, observation.AppID, observation.IdentityID, observation.ObservedAt, string(raw))
		return mapDBErr(err)
	}
	return stateStore.withSSHObservationFile(true, func(observations *[]model.AppSSHObservation) error {
		for _, existing := range *observations {
			if existing.ID == observation.ID {
				return ErrConflict
			}
		}
		*observations = append(*observations, observation)
		return nil
	})
}

func (stateStore *Store) LatestAppSSHObservation(tenantID, appID string) (model.AppSSHObservation, error) {
	app, err := stateStore.GetApp(appID)
	if err != nil {
		return model.AppSSHObservation{}, err
	}
	if app.TenantID != tenantID || isDeletedApp(app) {
		return model.AppSSHObservation{}, ErrNotFound
	}
	if stateStore.usingDatabase() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		var raw []byte
		if err := stateStore.db.QueryRowContext(ctx, `SELECT body FROM fugue_app_ssh_observations WHERE tenant_id=$1 AND app_id=$2 ORDER BY observed_at DESC,id DESC LIMIT 1`, tenantID, appID).Scan(&raw); err != nil {
			return model.AppSSHObservation{}, mapDBErr(err)
		}
		var observation model.AppSSHObservation
		if err := json.Unmarshal(raw, &observation); err != nil {
			return model.AppSSHObservation{}, err
		}
		if observation.TenantID != tenantID || observation.AppID != appID {
			return model.AppSSHObservation{}, ErrConflict
		}
		return observation, nil
	}
	var result model.AppSSHObservation
	err = stateStore.withSSHObservationFile(false, func(observations *[]model.AppSSHObservation) error {
		for _, observation := range *observations {
			if observation.AppID != appID || observation.TenantID != tenantID {
				continue
			}
			if result.ID == "" || observation.ObservedAt.After(result.ObservedAt) || (observation.ObservedAt.Equal(result.ObservedAt) && observation.ID > result.ID) {
				result = observation
			}
		}
		if result.ID == "" {
			return ErrNotFound
		}
		return nil
	})
	return result, err
}

func validateSSHObservation(observation model.AppSSHObservation, identity model.AppSSHIdentity, now time.Time) error {
	if observation.ID == "" || observation.Scope != "public_endpoint" || strings.TrimSpace(observation.Observer) == "" || observation.ObservedAt.IsZero() || observation.ObservedAt.After(now.Add(5*time.Second)) || observation.ObservedAt.Before(now.Add(-time.Minute)) || !observation.ExpiresAt.After(observation.ObservedAt) || observation.ExpiresAt.After(observation.ObservedAt.Add(2*time.Minute)) {
		return ErrInvalidInput
	}
	if observation.IdentityMatched && (!observation.KeyExchangeVerified || observation.Fingerprint != identity.Fingerprint || observation.Algorithm != identity.Algorithm || observation.Reason != "identity_verified") {
		return ErrInvalidInput
	}
	if observation.KeyExchangeVerified {
		if observation.Algorithm == "" || !strings.HasPrefix(observation.Fingerprint, "SHA256:") || observation.IdentityMatched != (observation.Fingerprint == identity.Fingerprint && observation.Algorithm == identity.Algorithm) {
			return ErrInvalidInput
		}
		if !observation.IdentityMatched && observation.Reason != "identity_mismatch" {
			return ErrInvalidInput
		}
	} else {
		if observation.IdentityMatched || observation.Fingerprint != "" || observation.Algorithm != "" {
			return ErrInvalidInput
		}
		switch observation.Reason {
		case "target_unreachable", "handshake_failed", "unverified":
		default:
			return ErrInvalidInput
		}
	}
	return nil
}

func (stateStore *Store) withSSHObservationFile(write bool, callback func(*[]model.AppSSHObservation) error) error {
	path := stateStore.path + ".ssh-observations.json"
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	observations := []model.AppSSHObservation{}
	raw, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err == nil {
		if err := json.Unmarshal(raw, &observations); err != nil {
			return err
		}
	}
	if err := callback(&observations); err != nil {
		return err
	}
	if !write {
		return nil
	}
	sort.Slice(observations, func(first, second int) bool {
		return observations[first].ObservedAt.After(observations[second].ObservedAt)
	})
	counts := map[string]int{}
	retained := observations[:0]
	for _, observation := range observations {
		if counts[observation.AppID] < 128 {
			retained = append(retained, observation)
			counts[observation.AppID]++
		}
	}
	raw, err = json.Marshal(retained)
	if err != nil {
		return err
	}
	return writeSSHIdentityFile(path, raw)
}
