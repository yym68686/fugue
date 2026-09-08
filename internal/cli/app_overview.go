package cli

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

type appOverviewSnapshot struct {
	Sources         map[string]evidenceSource     `json:"sources,omitempty"`
	Completeness    string                        `json:"completeness,omitempty"`
	MissingEvidence []string                      `json:"missing_evidence,omitempty"`
	App             model.App                     `json:"app"`
	Domains         []model.AppDomain             `json:"domains,omitempty"`
	Bindings        []model.ServiceBinding        `json:"bindings,omitempty"`
	BackingServices []model.BackingService        `json:"backing_services,omitempty"`
	Operations      []model.Operation             `json:"operations,omitempty"`
	ImageTracking   *model.AppImageTracking       `json:"image_tracking,omitempty"`
	Images          *appImageInventoryResponse    `json:"images,omitempty"`
	PodInventory    *model.AppRuntimePodInventory `json:"pod_inventory,omitempty"`
	Diagnosis       *appOverviewDiagnosis         `json:"diagnosis,omitempty"`
}

func (c *CLI) newAppOverviewCommand() *cobra.Command {
	opts := struct {
		ShowSecrets     bool
		RequireComplete bool
		Timeout         time.Duration
	}{Timeout: 10 * time.Second}
	cmd := &cobra.Command{
		Use:   "overview <app>",
		Short: "Show an app overview with related state",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			if opts.Timeout <= 0 {
				return fmt.Errorf("--timeout must be positive")
			}
			snapshot, err := c.loadAppOverviewWithin(client, args[0], opts.Timeout)
			if err != nil {
				return err
			}
			if err := c.renderAppOverviewSnapshot(client, snapshot, false, opts.ShowSecrets); err != nil {
				return err
			}
			if opts.RequireComplete && len(snapshot.MissingEvidence) > 0 {
				return withExitCode(fmt.Errorf("incomplete evidence: %s", strings.Join(snapshot.MissingEvidence, ", ")), ExitCodeIndeterminate)
			}
			return nil
		},
	}
	cmd.Flags().DurationVar(&opts.Timeout, "timeout", opts.Timeout, "Total budget for overview reads; unavailable sources remain explicit")
	cmd.Flags().BoolVar(&opts.RequireComplete, "require-complete", false, "Fail when any overview evidence source is unavailable")
	cmd.Flags().BoolVar(&opts.ShowSecrets, "show-secrets", false, "Show env values, passwords, and other sensitive fields")
	return cmd
}

func (c *CLI) newAppWatchCommand() *cobra.Command {
	opts := struct {
		Interval    time.Duration
		ShowSecrets bool
	}{Interval: 5 * time.Second}
	cmd := &cobra.Command{
		Use:   "watch <app>",
		Short: "Watch app overview changes",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()
			return c.watchAppOverview(ctx, client, args[0], opts.Interval, opts.ShowSecrets)
		},
	}
	cmd.Flags().DurationVar(&opts.Interval, "interval", opts.Interval, "Polling interval")
	cmd.Flags().BoolVar(&opts.ShowSecrets, "show-secrets", false, "Show env values, passwords, and other sensitive fields")
	return cmd
}

func (c *CLI) watchAppOverview(ctx context.Context, client *Client, ref string, interval time.Duration, showSecrets bool) error {
	var previousHash [32]byte
	first := true
	for {
		snapshot, hashValue, err := c.loadAppOverviewHash(client, ref)
		if err != nil {
			return err
		}
		if first || hashValue != previousHash {
			if err := c.renderAppOverviewSnapshot(client, snapshot, !first, showSecrets); err != nil {
				return err
			}
			previousHash = hashValue
			first = false
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(interval):
		}
	}
}

func (c *CLI) loadAppOverviewHash(client *Client, ref string) (appOverviewSnapshot, [32]byte, error) {
	snapshot, err := c.loadAppOverview(client, ref)
	if err != nil {
		return appOverviewSnapshot{}, [32]byte{}, err
	}
	hashView := snapshot
	hashView.Sources = map[string]evidenceSource{}
	for name, source := range snapshot.Sources {
		source.ObservedAt = time.Time{}
		hashView.Sources[name] = source
	}
	sum, err := json.Marshal(hashView)
	if err != nil {
		return appOverviewSnapshot{}, [32]byte{}, err
	}
	return snapshot, sha256.Sum256(sum), nil
}

func (c *CLI) loadAppOverview(client *Client, ref string) (appOverviewSnapshot, error) {
	return c.loadAppOverviewWithin(client, ref, 10*time.Second)
}

func selectPrimaryOverviewDiagnosis(primary, runtime *appOverviewDiagnosis) *appOverviewDiagnosis {
	switch {
	case primary == nil:
		return runtime
	case runtime == nil:
		return primary
	}
	switch strings.TrimSpace(primary.Category) {
	case "", "state-summary":
		return runtime
	default:
		return primary
	}
}

func (c *CLI) renderAppOverviewSnapshot(client *Client, snapshot appOverviewSnapshot, separate bool, showSecrets bool) error {
	if separate {
		if _, err := fmt.Fprintln(c.stdout); err != nil {
			return err
		}
	}
	if !showSecrets {
		snapshot = redactOverviewSnapshotForOutput(snapshot)
	}
	if c.wantsJSON() {
		return c.writeJSON(snapshot)
	}
	if c.shouldUseRichText() {
		if err := c.renderRichAppHealth(buildAppOverviewHealthView(snapshot)); err != nil {
			return err
		}
		if snapshot.Diagnosis != nil {
			if _, err := fmt.Fprintln(c.stdout); err != nil {
				return err
			}
			return c.renderRichDiagnosis(buildAppOverviewDiagnosisEvidenceView(snapshot.Diagnosis))
		}
		return nil
	}
	if _, err := fmt.Fprintf(c.stdout, "observed_at=%s\n", formatTime(time.Now().UTC())); err != nil {
		return err
	}
	if err := c.renderAppStatus(client, snapshot.App); err != nil {
		return err
	}
	if snapshot.Diagnosis != nil {
		if _, err := fmt.Fprintln(c.stdout); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(c.stdout, "diagnosis"); err != nil {
			return err
		}
		if err := renderAppOverviewDiagnosis(c.stdout, snapshot.Diagnosis); err != nil {
			return err
		}
	}
	if len(snapshot.Domains) > 0 {
		if _, err := fmt.Fprintln(c.stdout); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(c.stdout, "domains"); err != nil {
			return err
		}
		if err := writeDomainTable(c.stdout, snapshot.Domains); err != nil {
			return err
		}
	}
	if len(snapshot.Bindings) > 0 {
		if _, err := fmt.Fprintln(c.stdout); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(c.stdout, "services"); err != nil {
			return err
		}
		if err := writeBindingTable(c.stdout, snapshot.Bindings, snapshot.BackingServices); err != nil {
			return err
		}
	}
	if snapshot.Images != nil {
		if _, err := fmt.Fprintln(c.stdout); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(c.stdout, "images"); err != nil {
			return err
		}
		if err := writeKeyValues(c.stdout,
			kvPair{Key: "versions", Value: formatInt(snapshot.Images.Summary.VersionCount)},
			kvPair{Key: "current", Value: formatInt(snapshot.Images.Summary.CurrentVersionCount)},
			kvPair{Key: "stale", Value: formatInt(snapshot.Images.Summary.StaleVersionCount)},
			kvPair{Key: "reclaimable", Value: formatBytes(snapshot.Images.Summary.ReclaimableSizeBytes)},
		); err != nil {
			return err
		}
		if len(snapshot.Images.Versions) > 0 {
			if err := writeAppImageTable(c.stdout, snapshot.Images.Versions); err != nil {
				return err
			}
		}
	}
	if snapshot.ImageTracking != nil {
		if _, err := fmt.Fprintln(c.stdout); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(c.stdout, "image_tracking"); err != nil {
			return err
		}
		if err := writeAppOverviewImageTracking(c.stdout, snapshot.App, *snapshot.ImageTracking); err != nil {
			return err
		}
	}
	if snapshot.PodInventory != nil {
		for _, warning := range snapshot.PodInventory.Warnings {
			c.progressf("warning=%s", warning)
		}
		if _, err := fmt.Fprintln(c.stdout); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(c.stdout, "pods"); err != nil {
			return err
		}
		if err := renderAppRuntimePodInventory(c.stdout, *snapshot.PodInventory); err != nil {
			return err
		}
	}
	if len(snapshot.Operations) > 0 {
		if _, err := fmt.Fprintln(c.stdout); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(c.stdout, "operations"); err != nil {
			return err
		}
		if err := writeOperationTableWithApps(c.stdout, snapshot.Operations, mapAppNames([]model.App{snapshot.App})); err != nil {
			return err
		}
	}
	return nil
}

func writeAppOverviewImageTracking(w io.Writer, app model.App, tracking model.AppImageTracking) error {
	pairs := []kvPair{
		{Key: "enabled", Value: fmt.Sprintf("%t", tracking.Enabled)},
		{Key: "image_ref", Value: tracking.ImageRef},
		{Key: "last_seen_digest", Value: tracking.LastSeenDigest},
		{Key: "last_queued_digest", Value: tracking.LastQueuedDigest},
		{Key: "last_deployed_digest", Value: tracking.LastDeployedDigest},
		{Key: "last_operation_id", Value: tracking.LastOperationID},
		{Key: "last_event", Value: tracking.LastEvent},
		{Key: "last_checked_at", Value: formatOptionalTimePtr(tracking.LastCheckedAt)},
		{Key: "last_triggered_at", Value: formatOptionalTimePtr(tracking.LastTriggeredAt)},
	}
	if strings.TrimSpace(tracking.LastDeliveryID) != "" {
		pairs = append(pairs, kvPair{Key: "last_delivery_id", Value: tracking.LastDeliveryID})
	}
	if strings.TrimSpace(tracking.LastError) != "" {
		pairs = append(pairs, kvPair{Key: "last_error", Value: tracking.LastError})
	}
	if tracking.Enabled {
		appRef := firstNonEmptyTrimmed(app.Name, tracking.AppID)
		pairs = append(pairs, kvPair{Key: "sync_now", Value: "fugue app image tracking sync " + shellSingleQuote(appRef)})
	}
	return writeKeyValues(w, pairs...)
}
