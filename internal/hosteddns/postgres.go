package hosteddns

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"time"

	"fugue/internal/model"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type PostgresWorker struct {
	database *sql.DB
}

type Progress struct {
	Owner       string
	CompletedAt sql.NullTime
	Updated     sql.NullInt64
	Cursor      string
}

func OpenPostgresWorker(ctx context.Context, dsn string) (*PostgresWorker, error) {
	if dsn == "" {
		return nil, errors.New("hosted DNS worker database is required")
	}
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, errors.New("hosted DNS worker database could not be opened")
	}
	database.SetMaxOpenConns(2) // One reconciliation and one health check.
	database.SetMaxIdleConns(2)
	database.SetConnMaxIdleTime(5 * time.Minute)
	worker := &PostgresWorker{database: database}
	if _, err := worker.Progress(ctx); err != nil {
		database.Close()
		return nil, errors.New("hosted DNS worker database capability is unavailable")
	}
	return worker, nil
}

func (w *PostgresWorker) Close() error {
	return w.database.Close()
}

func (w *PostgresWorker) Progress(ctx context.Context) (Progress, error) {
	return readProgress(w.database.QueryRowContext(ctx, `SELECT * FROM fugue_hosted_dns_maintenance.progress_v1(NULL,NULL)`))
}

func readProgress(row *sql.Row) (Progress, error) {
	var progress Progress
	err := row.Scan(&progress.Owner, &progress.CompletedAt, &progress.Updated, &progress.Cursor)
	if err != nil {
		return Progress{}, err
	}
	if progress.Owner != "api" && progress.Owner != "worker" {
		return Progress{}, errors.New("unknown hosted DNS writer")
	}
	return progress, nil
}

const runLock = "hosted-dns-flatten/v1/run"

// Reconcile returns whether the current sweep has another page to inspect.
func (w *PostgresWorker) Reconcile(ctx context.Context) (bool, error) {
	conn, err := w.database.Conn(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close()
	var locked bool
	if err := conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock(hashtextextended($1,0))`, runLock).Scan(&locked); err != nil || !locked {
		return false, err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if _, err := conn.ExecContext(cleanup, `SELECT pg_advisory_unlock(hashtextextended($1,0))`, runLock); err != nil {
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
	}()
	progress, err := readProgress(conn.QueryRowContext(ctx, `SELECT * FROM fugue_hosted_dns_maintenance.progress_v1(NULL,NULL)`))
	if err != nil || progress.Owner != "worker" {
		return false, err
	}
	rows, err := conn.QueryContext(ctx, `SELECT * FROM fugue_hosted_dns_maintenance.records_v1($1)`, progress.Cursor)
	if err != nil {
		return false, err
	}
	type input struct {
		Zone   model.HostedZone `json:"zone"`
		Record model.DNSRecord  `json:"record"`
	}
	page, err := jsonRows[input](rows)
	if err != nil {
		return false, err
	}
	if len(page) == 0 {
		_, err = readProgress(conn.QueryRowContext(ctx, `SELECT * FROM fugue_hosted_dns_maintenance.progress_v1(0,'')`))
		return false, err
	}
	resolver := NewResolver()
	resolve := func(parent context.Context, name string, record model.DNSRecord) Result {
		queryCtx, cancel := context.WithTimeout(parent, 4*time.Second)
		defer cancel()
		return resolver.Resolve(queryCtx, name, record)
	}
	var pending pendingObservations
	cache := make(map[string]Result, len(page))
	for _, item := range page {
		if _, err := refreshRecord(ctx, &pending, item.Zone, item.Record, time.Now().UTC(), resolve, cache); err != nil {
			return false, err
		}
	}
	// Resolve before starting the transaction. An interrupted page must not
	// persist answers without its cursor, and DNS calls must not hold row locks.
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	writer := workerStore{transaction: tx}
	updated := 0
	for _, item := range pending {
		changed, err := writer.CommitHostedDNSFlattenObservation(ctx, item.zone, item.record, item.resolved)
		if err != nil {
			return false, err
		}
		if changed {
			updated++
		}
	}
	if _, err := readProgress(tx.QueryRowContext(ctx, `SELECT * FROM fugue_hosted_dns_maintenance.progress_v1($1,$2)`, updated, page[len(page)-1].Record.ID)); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

type pendingObservation struct {
	zone             model.HostedZone
	record, resolved model.DNSRecord
}

type pendingObservations []pendingObservation

func (p *pendingObservations) CommitHostedDNSFlattenObservation(_ context.Context, zone model.HostedZone, record, resolved model.DNSRecord) (bool, error) {
	*p = append(*p, pendingObservation{zone: zone, record: record, resolved: resolved})
	return true, nil
}

// workerStore writes observations and their cursor in the same page transaction.
type workerStore struct {
	transaction *sql.Tx
}

func (w workerStore) CommitHostedDNSFlattenObservation(ctx context.Context, zone model.HostedZone, observed, resolved model.DNSRecord) (bool, error) {
	// Marshal non-nil slices so an empty answer is written as [] rather than
	// null. DNS selection and address filtering are shared with API capture.
	a, err := json.Marshal(append([]string{}, resolved.FlattenedA...))
	if err != nil {
		return false, err
	}
	aaaa, err := json.Marshal(append([]string{}, resolved.FlattenedAAAA...))
	if err != nil {
		return false, err
	}
	var changed bool
	err = w.transaction.QueryRowContext(ctx, `
SELECT fugue_hosted_dns_maintenance.commit_v1($1,$2,$3,$4,$5,$6,$7::jsonb,$8::jsonb,$9,$10,$11,$12,$13)
`, zone.ID, zone.TenantID, zone.ProjectID, observed.ID, observed.CreatedAt, observed.UpdatedAt,
		string(a), string(aaaa), resolved.FlattenStatus, resolved.Status, resolved.LastResolvedAt, resolved.ResolveError, resolved.LastMessage).Scan(&changed)
	return changed, err
}

func jsonRows[T any](rows *sql.Rows) ([]T, error) {
	defer rows.Close()
	var values []T
	for rows.Next() {
		var raw []byte
		var value T
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}
