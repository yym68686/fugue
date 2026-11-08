package cli

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
)

func (c *CLI) waitForPublicApp(client *Client, app *model.App) error {
	if app.Spec.Replicas <= 0 || model.AppUsesBackgroundNetwork(app.Spec) || model.AppUsesInternalNetwork(app.Spec) || app.Route == nil || strings.TrimSpace(app.Route.PublicURL) == "" {
		return nil
	}
	ctx := client.context
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Minute)
	defer cancel()
	observingClient := *client
	observingClient.context = ctx
	// A deployment response without a fresh runtime observation is not enough
	// evidence that this app owns a live public route. The control plane may
	// still be returning a cached route while the operation settles; refresh the
	// app until its observation is fresh before probing the public endpoint.
	transientErrors := deployWaitTransientErrorTracker{}
	for app.ObservedStatus == nil || !app.ObservedStatus.Fresh {
		if err := ctx.Err(); err != nil {
			return withExitCode(fmt.Errorf("deployment operation completed; runtime observation remains stale: %w", err), ExitCodeIndeterminate)
		}
		if strings.TrimSpace(app.ID) == "" {
			return withExitCode(errors.New("deployment operation completed; app identity is missing while waiting for runtime observation"), ExitCodeIndeterminate)
		}
		refreshed, err := observingClient.GetApp(app.ID)
		if err != nil {
			retry, retryErr := transientErrors.shouldRetry(c, err)
			if retryErr != nil {
				return withExitCode(fmt.Errorf("deployment operation completed; runtime observation is unavailable: %w", retryErr), ExitCodeIndeterminate)
			}
			if retry {
				if err := waitRequestRetry(ctx, deployWaitPollInterval); err != nil {
					return withExitCode(fmt.Errorf("deployment operation completed; runtime observation remains stale: %w", err), ExitCodeIndeterminate)
				}
				continue
			}
		}
		transientErrors.reset()
		if refreshed.ID != app.ID || refreshed.Route == nil || refreshed.Route.PublicURL != app.Route.PublicURL {
			return withExitCode(errors.New("deployment operation completed; app identity or public route changed while waiting for runtime observation"), ExitCodeIndeterminate)
		}
		*app = refreshed
		if app.ObservedStatus == nil || !app.ObservedStatus.Fresh {
			if err := waitRequestRetry(ctx, deployWaitPollInterval); err != nil {
				return withExitCode(fmt.Errorf("deployment operation completed; runtime observation remains stale: %w", err), ExitCodeIndeterminate)
			}
		}
	}
	if publicServingState(*app).Status == "not_ready" {
		return withExitCode(errors.New("deployment operation completed; runtime observation is not ready"), ExitCodeIndeterminate)
	}
	return c.waitForPublicURL(ctx, app.Route.PublicURL, nil)
}

func (c *CLI) waitForPublicURL(ctx context.Context, target string, transport *http.Client) error {
	lastReason := ""
	for {
		if err := ctx.Err(); err != nil {
			return withExitCode(fmt.Errorf("deployment operation completed; public availability remains unverified (%s): %w", lastReason, err), ExitCodeIndeterminate)
		}
		observed := httpx.ObservePublicRoute(ctx, target, transport)
		if observed.Reachable {
			c.progressf("public_route_ready=true public_status=%d", observed.StatusCode)
			return nil
		}
		if observed.Reason != lastReason {
			c.progressf("public_route_ready=false reason=%s; waiting for public availability", observed.Reason)
			lastReason = observed.Reason
		}
		if err := waitRequestRetry(ctx, deployWaitPollInterval); err != nil {
			return withExitCode(fmt.Errorf("deployment operation completed; public availability remains unverified (%s): %w", lastReason, err), ExitCodeIndeterminate)
		}
	}
}
