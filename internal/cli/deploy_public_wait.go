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
	return c.waitForPublicAppWithProbe(client, app, func(ctx context.Context, target string) error {
		return c.waitForPublicURL(ctx, target, nil)
	})
}

func (c *CLI) waitForPublicAppWithProbe(client *Client, app *model.App, probe func(context.Context, string) error) error {
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
	if err := c.refreshPublicAppObservation(&observingClient, app); err != nil {
		return err
	}
	if err := probe(ctx, app.Route.PublicURL); err != nil {
		return err
	}
	// Public publication can take longer than the runtime evidence lifetime.
	// Retain a current observation for the final result rather than reporting
	// unknown from the snapshot obtained before waiting for the route.
	return c.refreshPublicAppObservation(&observingClient, app)
}

func (c *CLI) refreshPublicAppObservation(observingClient *Client, app *model.App) error {
	ctx := observingClient.context
	// A deployment response without a fresh runtime observation is not enough
	// evidence that this app owns a live public route. The control plane may
	// still be returning a cached route while the operation settles; refresh the
	// app until its observation is fresh before probing the public endpoint.
	transientErrors := deployWaitTransientErrorTracker{}
	for publicServingState(*app).Status == "unknown" {
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
		if publicServingState(*app).Status == "unknown" {
			if err := waitRequestRetry(ctx, deployWaitPollInterval); err != nil {
				return withExitCode(fmt.Errorf("deployment operation completed; runtime observation remains stale: %w", err), ExitCodeIndeterminate)
			}
		}
	}
	if publicServingState(*app).Status == "not_ready" {
		return withExitCode(errors.New("deployment operation completed; runtime observation is not ready"), ExitCodeIndeterminate)
	}
	return nil
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
