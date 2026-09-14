package api

import "context"

// Completion covers the entire callback, including a store write already in
// progress when cancellation arrives. Callers can join before closing storage.
func startWarmerTask(ctx context.Context, work func()) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		if ctx.Err() == nil {
			work()
		}
	}()
	return done
}

func joinWarmerTasks(tasks ...<-chan struct{}) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, task := range tasks {
			<-task
		}
	}()
	return done
}
