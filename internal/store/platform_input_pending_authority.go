package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"hash"
	"time"

	"fugue/internal/inputarchive"
	"fugue/internal/schemamigrate"
)

type compilerInputPendingPolicyWitness struct {
	revision, relation, filenode int64
	xmin, ctid, raw, leaseID     string
	expiresUnix                  int64
}

type compilerInputPendingRegistryWitness struct {
	hash  string
	bytes int
}

func readPendingRootPolicy(ctx context.Context, tx *sql.Tx, authority inputarchive.PendingRootAuthority, lock bool) (compilerInputPendingPolicyWitness, error) {
	var w compilerInputPendingPolicyWitness
	if err := schemamigrate.VerifyCompilerInputPendingAuthorities(ctx, tx); err != nil {
		return w, err
	}
	query := `SELECT singleton,revision,left(policy_json,65537),tableoid::bigint,pg_relation_filenode(tableoid),xmin::text,ctid::text FROM fugue_platform_input_pending_authorities LIMIT 2`
	if lock {
		query += ` FOR SHARE`
	}
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return w, err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
		var singleton bool
		if count > 1 {
			return w, ErrConflict
		}
		if err := rows.Scan(&singleton, &w.revision, &w.raw, &w.relation, &w.filenode, &w.xmin, &w.ctid); err != nil {
			return w, err
		}
		if !singleton || w.revision < 1 {
			return w, ErrConflict
		}
	}
	if err := rows.Err(); err != nil {
		return w, err
	}
	if count != 1 {
		return w, ErrConflict
	}
	policy, err := inputarchive.DecodePendingRootAuthorityPolicy([]byte(w.raw))
	if err != nil {
		return w, ErrConflict
	}
	for _, lease := range policy.Leases {
		if lease.Authority == authority {
			w.leaseID, w.expiresUnix = lease.LeaseID, lease.ExpiresUnix
		}
	}
	if w.leaseID == "" {
		return w, ErrConflict
	}
	// Native server time is authoritative; transaction_timestamp is insufficient
	// after a lock wait. This is checked again immediately before commit.
	if err := checkPendingRootLeaseTime(ctx, tx, w.expiresUnix); err != nil {
		return w, err
	}
	return w, nil
}

func checkPendingRootLeaseTime(ctx context.Context, tx *sql.Tx, expiry int64) error {
	var live bool
	if err := tx.QueryRowContext(ctx, `SELECT clock_timestamp()<to_timestamp($1)`, expiry).Scan(&live); err != nil {
		return err
	}
	if !live || !time.Now().Before(time.Unix(expiry, 0)) {
		return ErrConflict
	}
	return ctx.Err()
}

// Complete bounded registry read in the SAME preparation RR as the native
// signed dependency closure. Hash every record and concrete binding, including
// physical tuple identities; max(revision) or a chosen root subset is not a
// complete witness. Charge actual encoded bytes to the existing first batch.
func readPendingRootRegistry(ctx context.Context, tx *sql.Tx) (compilerInputPendingRegistryWitness, error) {
	w := compilerInputPendingRegistryWitness{}
	if err := schemamigrate.VerifyCompilerInputExternalRoots(ctx, tx); err != nil {
		return w, err
	}
	h := sha256.New()
	for index, query := range []string{
		`SELECT registration_sha256,registry_revision,owner_key,generation,source_digest,source_epoch,left(registration_json,16385),tableoid::bigint,pg_relation_filenode(tableoid),xmin::text,ctid::text FROM fugue_platform_input_external_roots ORDER BY registry_revision LIMIT 1025`,
		`SELECT registration_sha256,ordinal,artifact_id,release_id,content_hash,tableoid::bigint,pg_relation_filenode(tableoid),xmin::text,ctid::text FROM fugue_platform_input_external_root_refs ORDER BY registration_sha256,ordinal LIMIT 32769`,
	} {
		// An empty table still has a physical identity. Replacing it must not
		// preserve an apparently empty registry witness.
		table := []string{"fugue_platform_input_external_roots", "fugue_platform_input_external_root_refs"}[index]
		var relation, filenode int64
		if err := tx.QueryRowContext(ctx, `SELECT $1::regclass::oid::bigint,pg_relation_filenode($1::regclass)`, table).Scan(&relation, &filenode); err != nil {
			return w, err
		}
		if err := pendingRegistryHashRow(h, &w.bytes, index, []any{relation, filenode}); err != nil {
			return w, err
		}
		rows, err := tx.QueryContext(ctx, query)
		if err != nil {
			return w, err
		}
		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			return w, err
		}
		count := 0
		for rows.Next() {
			count++
			if index == 0 && count > compilerInputExternalRegistryLimit || index == 1 && count > compilerInputExternalReferenceLimit {
				rows.Close()
				return w, ErrConflict
			}
			values, pointers := make([]any, len(columns)), make([]any, len(columns))
			for i := range values {
				pointers[i] = &values[i]
			}
			if err := rows.Scan(pointers...); err != nil {
				rows.Close()
				return w, err
			}
			if err := pendingRegistryHashRow(h, &w.bytes, index, values); err != nil {
				rows.Close()
				return w, err
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return w, err
		}
	}
	w.hash = hex.EncodeToString(h.Sum(nil))
	return w, nil
}

func pendingRegistryHashRow(h hash.Hash, bytes *int, table int, row []any) error {
	raw, err := json.Marshal(struct {
		Table int
		Row   []any
	}{table, row})
	if err != nil || len(raw) > (8<<20)-*bytes {
		return ErrConflict
	}
	*bytes += len(raw) + 1
	if *bytes > 8<<20 {
		return ErrConflict
	}
	h.Write(raw)
	h.Write([]byte{'\n'})
	return nil
}

func preparePendingRootNativeWitness(ctx context.Context, tx *sql.Tx, intent *compilerInputPendingRootIntent) (int, error) {
	var err error
	intent.policy, err = readPendingRootPolicy(ctx, tx, intent.authority, false)
	if err != nil {
		return 0, err
	}
	// Also validate the full canonical registrations under existing owner caps.
	if _, _, err = compilerInputExternalRoots(ctx, tx, compilerInputHotRootLimit, nil); err != nil {
		return 0, err
	}
	intent.registry, err = readPendingRootRegistry(ctx, tx)
	// The canonical registration pass is separate from the physical hash pass;
	// charge it conservatively at the latter's full encoded size as well.
	return len(intent.policy.raw) + 2*intent.registry.bytes, err
}

func checkPendingRootNativeWitness(ctx context.Context, tx *sql.Tx, intent *compilerInputPendingRootIntent) error {
	policy, err := readPendingRootPolicy(ctx, tx, intent.authority, true)
	if err != nil {
		return err
	}
	if policy != intent.policy {
		return ErrConflict
	}
	registry, err := readPendingRootRegistry(ctx, tx)
	if err != nil {
		return err
	}
	if registry != intent.registry {
		return ErrConflict
	}
	return nil
}
