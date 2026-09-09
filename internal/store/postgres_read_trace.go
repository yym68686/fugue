package store

import (
	"context"
	"database/sql"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

type readStageNameKey struct{}
type queryTraceStartKey struct{}
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
type postgresReadTracer struct{}

func (postgresReadTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	if ctx.Value(readStageObserverKey{}) == nil {
		return ctx
	}
	return context.WithValue(ctx, queryTraceStartKey{}, time.Now())
}

func (postgresReadTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
	if started, ok := ctx.Value(queryTraceStartKey{}).(time.Time); ok {
		recordReadStage(ctx, readStageName(ctx)+"_query", started)
	}
}

func (postgresReadTracer) TracePrepareStart(ctx context.Context, _ *pgx.Conn, _ pgx.TracePrepareStartData) context.Context {
	if ctx.Value(readStageObserverKey{}) == nil {
		return ctx
	}
	return context.WithValue(ctx, prepareTraceStartKey{}, time.Now())
}

func (postgresReadTracer) TracePrepareEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TracePrepareEndData) {
	if started, ok := ctx.Value(prepareTraceStartKey{}).(time.Time); ok {
		recordReadStage(ctx, readStageName(ctx)+"_prepare", started)
	}
}

func openPostgresDatabase(address string) (*sql.DB, error) {
	config, err := pgx.ParseConfig(address)
	if err != nil {
		return nil, err
	}
	config.Tracer = postgresReadTracer{}
	return stdlib.OpenDB(*config), nil
}
