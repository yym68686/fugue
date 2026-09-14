package api

import (
	"context"
	"testing"
	"time"
)

func TestWarmerCompletionWaitsForInflightWork(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started, release := make(chan struct{}), make(chan struct{})
	done := startWarmerTask(ctx, func() { close(started); <-release })
	<-started
	cancel()
	select {
	case <-done:
		t.Fatal("completion returned before the pending write completed")
	default:
	}
	close(release)
	select {
	case <-joinWarmerTasks(done):
	case <-time.After(time.Second):
		t.Fatal("finished warmer did not close completion")
	}
}

func TestCancelledWarmersDoNotStartNewWork(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := false
	<-startWarmerTask(ctx, func() { started = true })
	if started {
		t.Fatal("cancelled task started")
	}
	var server *Server
	select {
	case <-server.StartBackgroundWarmers(ctx):
	case <-time.After(time.Second):
		t.Fatal("disabled warmers did not complete")
	}
}
