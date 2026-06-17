package edge

import (
	"context"
	"net"

	"fugue/internal/tcpdiag"
)

type edgeDownstreamConnContextKey struct{}

func edgeContextWithDownstreamConn(ctx context.Context, conn net.Conn) context.Context {
	if ctx == nil || conn == nil {
		return ctx
	}
	return context.WithValue(ctx, edgeDownstreamConnContextKey{}, conn)
}

func edgeDownstreamConnFromContext(ctx context.Context) net.Conn {
	if ctx == nil {
		return nil
	}
	conn, _ := ctx.Value(edgeDownstreamConnContextKey{}).(net.Conn)
	return conn
}

func edgeTCPInfoSnapshotFromContext(ctx context.Context) tcpdiag.Snapshot {
	return tcpdiag.SnapshotFromConn(edgeDownstreamConnFromContext(ctx))
}

func edgeTCPInfoDebug(snapshot tcpdiag.Snapshot) map[string]any {
	return tcpdiag.SnapshotFields("", snapshot)
}
