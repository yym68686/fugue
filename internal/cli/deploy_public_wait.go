package cli

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
)

func (c *CLI) waitForPublicApp(client *Client, app model.App) error {
	if app.Spec.Replicas <= 0 || model.AppUsesBackgroundNetwork(app.Spec) || model.AppUsesInternalNetwork(app.Spec) || app.Route == nil || strings.TrimSpace(app.Route.PublicURL) == "" {
		return nil
	}
	// A deployment response without a fresh runtime observation is not enough
	// evidence that this app owns a live public route. The control plane may
	// still be returning a cached route while the operation settles; wait for
	// its next observed status before turning a route probe into a deployment
	// outcome.
	if app.ObservedStatus == nil || !app.ObservedStatus.Fresh {
		return nil
	}
	ctx := client.context
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Minute)
	defer cancel()
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
