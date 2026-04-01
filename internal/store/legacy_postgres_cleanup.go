package store

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

const legacyTenantDataRoot = "/var/lib/fugue/tenant-data"

// LegacyPostgresStoragePaths returns the app-owned legacy hostPath locations that
// are no longer used once managed postgres runs on PVCs.
func LegacyPostgresStoragePaths(app model.App) []string {
	unique := make(map[string]struct{})
	addPath := func(raw string) {
		cleaned := normalizedLegacyPostgresStoragePath(raw)
		if cleaned == "" {
			return
		}
		unique[cleaned] = struct{}{}
	}

	if app.Spec.Postgres != nil {
		addPath(app.Spec.Postgres.StoragePath)
	}
	for _, service := range app.BackingServices {
		ownerAppID := strings.TrimSpace(service.OwnerAppID)
		if ownerAppID != "" && ownerAppID != strings.TrimSpace(app.ID) {
			continue
		}
		if service.Spec.Postgres == nil {
			continue
		}
		addPath(service.Spec.Postgres.StoragePath)
	}

	if len(unique) == 0 {
		return nil
	}
	out := make([]string, 0, len(unique))
	for storagePath := range unique {
		out = append(out, storagePath)
	}
	sort.Strings(out)
	return out
}

func normalizedLegacyPostgresStoragePath(raw string) string {
	cleaned := path.Clean(strings.TrimSpace(raw))
	if cleaned == "" || cleaned == "." || cleaned == "/" {
		return ""
	}
	if !strings.HasPrefix(cleaned, legacyTenantDataRoot+"/") {
		return ""
	}
	return cleaned
}

func clearLegacyPostgresStoragePathFromApp(app *model.App) bool {
	if app == nil || app.Spec.Postgres == nil {
		return false
	}
	if strings.TrimSpace(app.Spec.Postgres.StoragePath) == "" {
		return false
	}
	app.Spec.Postgres.StoragePath = ""
	return true
}

func clearLegacyPostgresStoragePathFromService(service *model.BackingService) bool {
	if service == nil || service.Spec.Postgres == nil {
		return false
	}
	if strings.TrimSpace(service.Spec.Postgres.StoragePath) == "" {
		return false
	}
	service.Spec.Postgres.StoragePath = ""
	return true
}

func (s *Store) ClearLegacyPostgresStoragePaths(appID string) (bool, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return false, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgClearLegacyPostgresStoragePaths(appID)
	}

	var changed bool
	err := s.withLockedState(true, func(state *model.State) error {
		appIndex := findApp(state, appID)
		if appIndex < 0 {
			return ErrNotFound
		}

		now := time.Now().UTC()
		if clearLegacyPostgresStoragePathFromApp(&state.Apps[appIndex]) {
			state.Apps[appIndex].UpdatedAt = now
			changed = true
		}
		for index := range state.BackingServices {
			if strings.TrimSpace(state.BackingServices[index].OwnerAppID) != appID {
				continue
			}
			if clearLegacyPostgresStoragePathFromService(&state.BackingServices[index]) {
				state.BackingServices[index].UpdatedAt = now
				changed = true
			}
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return changed, nil
}

func (s *Store) pgClearLegacyPostgresStoragePaths(appID string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin clear legacy postgres storage paths transaction: %w", err)
	}
	defer tx.Rollback()

	app, err := s.pgGetAppTx(ctx, tx, appID, true)
	if err != nil {
		return false, mapDBErr(err)
	}

	now := time.Now().UTC()
	changed := false
	if clearLegacyPostgresStoragePathFromApp(&app) {
		app.UpdatedAt = now
		if err := s.pgUpdateAppTx(ctx, tx, app); err != nil {
			return false, err
		}
		changed = true
	}

	service, found, err := s.pgGetOwnedBackingServiceByAppAndTypeTx(ctx, tx, appID, model.BackingServiceTypePostgres, true)
	if err != nil {
		return false, err
	}
	if found && clearLegacyPostgresStoragePathFromService(&service) {
		service.UpdatedAt = now
		if err := s.pgUpdateBackingServiceTx(ctx, tx, service); err != nil {
			return false, err
		}
		changed = true
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit clear legacy postgres storage paths transaction: %w", err)
	}
	return changed, nil
}
