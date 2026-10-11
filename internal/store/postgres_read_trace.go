package store

import (
	"context"
	"database/sql"
	"fugue/internal/runtimeobservation"
	"sync"
	"time"
	"weak"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

type readStageNameKey struct{}
type queryTraceStartKey struct{}
type databaseQueryKey struct{}
type databaseConnectionFact struct {
	sequence uint64
	observed time.Time
}
type prepareTraceStartKey struct{}

func withReadStageName(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, readStageNameKey{}, name)
}

func readStageName(ctx context.Context) string {
	if name, ok := ctx.Value(readStageNameKey{}).(string); ok {
		return name
	}
	return "database"
}

// The driver observer never retains query text, parameters or connection
// settings. It separates actual protocol preparation from row consumption.
type postgresReadTracer struct {
	mu          sync.Mutex
	next        uint64
	connections map[weak.Pointer[pgx.Conn]]databaseConnectionFact
}

func (t *postgresReadTracer) connection(conn *pgx.Conn) databaseConnectionFact {
	if !t.mu.TryLock() {
		return databaseConnectionFact{}
	}
	defer t.mu.Unlock()
	if t.connections == nil {
		t.connections = map[weak.Pointer[pgx.Conn]]databaseConnectionFact{}
	}
	key := weak.Make(conn)
	if fact, ok := t.connections[key]; ok {
		return fact
	}
	if len(t.connections) >= 128 {
		var oldest weak.Pointer[pgx.Conn]
		found := false
		var order uint64
		for c, f := range t.connections {
			if !found || f.sequence < order {
				found = true
				oldest, order = c, f.sequence
			}
		}
		delete(t.connections, oldest)
	}
	t.next++
	fact := databaseConnectionFact{t.next, time.Now().UTC()}
	t.connections[key] = fact
	return fact
}

func (t *postgresReadTracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	connection := t.connection(conn)
	if conn != nil && connection.sequence != 0 {
		id := runtimeobservation.DatabaseObservations.Start(ctx, conn.PgConn().PID(), connection.sequence, connection.observed, data.SQL, data.Args)
		ctx = context.WithValue(ctx, databaseQueryKey{}, id)
	} else {
		runtimeobservation.DatabaseObservations.Drop()
	}
	if ctx.Value(readStageObserverKey{}) == nil {
		return ctx
	}
	return context.WithValue(ctx, queryTraceStartKey{}, time.Now())
}

func (*postgresReadTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	if id, ok := ctx.Value(databaseQueryKey{}).(uint64); ok {
		runtimeobservation.DatabaseObservations.Finish(id, data.Err != nil)
	}
	if started, ok := ctx.Value(queryTraceStartKey{}).(time.Time); ok {
		recordReadStage(ctx, readStageName(ctx)+"_query", started)
	}
}

func (*postgresReadTracer) TracePrepareStart(ctx context.Context, _ *pgx.Conn, _ pgx.TracePrepareStartData) context.Context {
	if ctx.Value(readStageObserverKey{}) == nil {
		return ctx
	}
	return context.WithValue(ctx, prepareTraceStartKey{}, time.Now())
}

func (*postgresReadTracer) TracePrepareEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TracePrepareEndData) {
	if started, ok := ctx.Value(prepareTraceStartKey{}).(time.Time); ok {
		recordReadStage(ctx, readStageName(ctx)+"_prepare", started)
	}
}

func openPostgresDatabase(address string) (*sql.DB, error) {
	config, err := pgx.ParseConfig(address)
	if err != nil {
		return nil, err
	}
	config.Tracer = &postgresReadTracer{}
	return stdlib.OpenDB(*config), nil
}
