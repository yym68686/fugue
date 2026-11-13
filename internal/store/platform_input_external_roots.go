package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fugue/internal/inputarchive"
	"fugue/internal/schemamigrate"
	"strconv"
)

// Read-only compatibility for installations that once enabled the retired
// append-only registry. Existing roots remain retention roots; this release
// neither installs that registry nor provides a writer or policy coordinator.
const compilerInputExternalRegistryLimit = 1024
const compilerInputExternalOwnerLimit = 32

func compilerInputExternalRoots(ctx context.Context, tx *sql.Tx, limit int, observe func(inputarchive.HotRoot, compilerInputRootState) error) ([]inputarchive.HotRoot, int64, error) {
	var installed bool
	if err := tx.QueryRowContext(ctx, `SELECT to_regclass('fugue_platform_input_external_roots') IS NOT NULL OR to_regclass('fugue_platform_input_external_root_refs') IS NOT NULL`).Scan(&installed); err != nil || !installed {
		return nil, 0, err
	}
	if err := schemamigrate.VerifyCompilerInputExternalRoots(ctx, tx); err != nil {
		return nil, 0, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT registration_sha256,registry_revision,owner_key,generation,source_digest,source_epoch,left(registration_json,16385) FROM fugue_platform_input_external_roots ORDER BY registry_revision LIMIT $1`, compilerInputExternalRegistryLimit+1)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	roots := []inputarchive.HotRoot{}
	owners := map[string]int{}
	var revision int64
	count := 0
	for rows.Next() {
		var hash, owner, generation, digest, raw string
		var next, epoch int64
		if err := rows.Scan(&hash, &next, &owner, &generation, &digest, &epoch, &raw); err != nil {
			return nil, 0, err
		}
		var registration inputarchive.PendingRootRegistration
		if json.Unmarshal([]byte(raw), &registration) != nil {
			return nil, 0, ErrConflict
		}
		canonical, actual, err := registration.Canonical()
		owners[owner]++
		count++
		if err != nil || actual != hash || string(canonical) != raw || registration.Owner != owner || registration.Generation != generation || registration.SourceDigest != digest ||
			next <= revision || epoch < 0 || count > compilerInputExternalRegistryLimit || owners[owner] > compilerInputExternalOwnerLimit || len(registration.References) > limit-len(roots) {
			return nil, 0, ErrConflict
		}
		revision = next
		for i, ref := range registration.References {
			root := inputarchive.HotRoot{Source: "pending-external-root", ID: hash + "/" + strconv.Itoa(i), Reference: ref}
			roots = append(roots, root)
			if observe != nil {
				if err := observe(root, compilerInputRootState{State: "pending", Generation: generation}); err != nil {
					return nil, 0, err
				}
			}
		}
	}
	return roots, revision, rows.Err()
}

// Final enforcement takes SHARE on both tables BEFORE re-reading the census.
// This also catches a root appended after preparation that happens to point to
// already-retained native inputs. A record's identity must not disappear just
// because its input set overlaps another root.
func lockCompilerInputExternalRootRegistry(ctx context.Context, tx *sql.Tx) error {
	var installed bool
	if err := tx.QueryRowContext(ctx, `SELECT to_regclass('fugue_platform_input_external_roots') IS NOT NULL OR to_regclass('fugue_platform_input_external_root_refs') IS NOT NULL`).Scan(&installed); err != nil || !installed {
		return err
	}
	if _, err := tx.ExecContext(ctx, `LOCK TABLE fugue_platform_input_external_roots,fugue_platform_input_external_root_refs IN SHARE MODE`); err != nil {
		return err
	}
	return schemamigrate.VerifyCompilerInputExternalRoots(ctx, tx)
}
