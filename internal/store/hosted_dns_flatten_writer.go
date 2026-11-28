package store

import (
	"context"
	"database/sql"
	"errors"
)

type HostedDNSFlattenWriter string

const (
	HostedDNSFlattenAPI      HostedDNSFlattenWriter = "api"
	HostedDNSFlattenWorker   HostedDNSFlattenWriter = "worker"
	hostedDNSFlattenOwnerKey                        = "hosted-dns-flatten/v1/writer"
)

func readHostedDNSFlattenWriter(ctx context.Context, db platformStateDB) (HostedDNSFlattenWriter, error) {
	var writer HostedDNSFlattenWriter
	err := db.QueryRowContext(ctx, `SELECT value FROM fugue_meta WHERE key=$1`, hostedDNSFlattenOwnerKey).Scan(&writer)
	if errors.Is(err, sql.ErrNoRows) {
		return HostedDNSFlattenAPI, nil
	}
	if err != nil {
		return "", err
	}
	if writer != HostedDNSFlattenAPI && writer != HostedDNSFlattenWorker {
		return "", errors.New("invalid hosted DNS flatten writer")
	}
	return writer, nil
}

func (s *Store) ActiveHostedDNSFlattenWriter(ctx context.Context) (HostedDNSFlattenWriter, error) {
	return readHostedDNSFlattenWriter(ctx, s.db)
}

func admitHostedDNSFlattenWriter(ctx context.Context, tx *sql.Tx, writer HostedDNSFlattenWriter) (bool, error) {
	var acquired bool
	err := tx.QueryRowContext(ctx, `SELECT pg_try_advisory_xact_lock_shared(hashtextextended($1,0))`, hostedDNSFlattenOwnerKey).Scan(&acquired)
	if err != nil || !acquired {
		return false, err
	}
	owner, err := readHostedDNSFlattenWriter(ctx, tx)
	return owner == writer, err
}
