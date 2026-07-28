package controller

import (
	"context"
	"log"
	"time"

	"fugue/internal/store"

	"github.com/jackc/pgx/v5"
)

func listenForOperationEvents(ctx context.Context, logger *log.Logger, databaseURL string) <-chan string {
	events := make(chan string, 16)
	if databaseURL == "" {
		close(events)
		return events
	}
	if logger == nil {
		logger = log.Default()
	}

	go func() {
		defer close(events)
		for {
			if ctx.Err() != nil {
				return
			}

			conn, err := pgx.Connect(ctx, databaseURL)
			if err != nil {
				logger.Printf("controller listen connect error: %v", err)
				if !sleepContext(ctx, 2*time.Second) {
					return
				}
				continue
			}

			if _, err := conn.Exec(ctx, "LISTEN "+store.PostgresOperationChannel); err != nil {
				logger.Printf("controller listen subscribe error: %v", err)
				_ = conn.Close(context.Background())
				if !sleepContext(ctx, 2*time.Second) {
					return
				}
				continue
			}

			for {
				if ctx.Err() != nil {
					_ = conn.Close(context.Background())
					return
				}
				notification, err := conn.WaitForNotification(ctx)
				if err != nil {
					logger.Printf("controller listen wait error: %v", err)
					_ = conn.Close(context.Background())
					break
				}
				if notification != nil {
					// The payload is the operation ID. Keep the notification
					// targeted so the controller can reconcile one app instead
					// of scanning every ManagedApp after each operation update.
					select {
					case events <- notification.Payload:
					default:
						// A full fallback scan will repair any notification that
						// was dropped while the controller was busy.
					}
				}
			}
		}
	}()

	return events
}

func sleepContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
