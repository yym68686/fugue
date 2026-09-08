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
	}{}
	cmd := &cobra.Command{
		Use:   "overview <app>",
		Short: "Show an app overview with related state",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			snapshot, err := c.loadAppOverview(client, args[0])
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
	sum, err := json.Marshal(snapshot)
	if err != nil {
		return appOverviewSnapshot{}, [32]byte{}, err
	}
	return snapshot, sha256.Sum256(sum), nil
}

func (c *CLI) loadAppOverview(client *Client, ref string) (appOverviewSnapshot, error) {
	app, err := c.resolveNamedApp(client, ref)
	if err != nil {
		return appOverviewSnapshot{}, err
	}
	app, err = client.GetApp(app.ID)
	if err != nil {
		return appOverviewSnapshot{}, err
	}
	snapshot := appOverviewSnapshot{App: app}
	if domains, err := client.ListAppDomains(app.ID); err != nil {
		snapshot.recordSource("domains", err, false)
		c.progressf("warning=domain inventory unavailable: %v", err)
	} else {
		snapshot.recordSource("domains", nil, len(domains) == 0)
		snapshot.Domains = domains
	}
	if bindings, err := client.ListAppBindings(app.ID); err != nil {
		snapshot.recordSource("bindings", err, false)
		c.progressf("warning=service binding inventory unavailable: %v", err)
	} else {
		snapshot.recordSource("bindings", nil, len(bindings.Bindings) == 0)
		snapshot.Bindings = bindings.Bindings
		snapshot.BackingServices = bindings.BackingServices
	}
	if operations, err := client.ListOperations(app.ID); err != nil {
		snapshot.recordSource("operations", err, false)
		c.progressf("warning=operation inventory unavailable: %v", err)
	} else {
		snapshot.recordSource("operations", nil, len(operations) == 0)
		snapshot.Operations = operations
	}
	if tracking, err := client.GetAppImageTracking(app.ID); err != nil {
		snapshot.recordSource("image_tracking", err, false)
		c.progressf("warning=image tracking unavailable: %v", err)
	} else {
		snapshot.recordSource("image_tracking", nil, tracking.Tracking == nil)
		snapshot.ImageTracking = tracking.Tracking
	}
	if images, err := client.GetAppImages(app.ID); err != nil {
		snapshot.recordSource("images", err, false)
		c.progressf("warning=image inventory unavailable: %v", err)
	} else {
		snapshot.recordSource("images", nil, len(images.Versions) == 0)
		snapshot.Images = &images
	}
	if podInventory, err := client.GetAppRuntimePods(app.ID, "app"); err != nil {
		snapshot.recordSource("pods", err, false)
		c.progressf("warning=runtime pod inventory unavailable: %v", err)
	} else {
		snapshot.recordSource("pods", nil, len(podInventory.Groups) == 0)
		snapshot.PodInventory = &podInventory
	}
	if diagnosis, err := c.buildAppOverviewDiagnosis(client, snapshot); err != nil {
		snapshot.recordSource("diagnosis", err, false)
		c.progressf("warning=app diagnosis unavailable: %v", err)
	} else {
		snapshot.recordSource("diagnosis", nil, diagnosis == nil)
		snapshot.Diagnosis = diagnosis
	}
	if runtimeDiagnosis, err := client.TryGetAppDiagnosis(app.ID, "app"); err != nil {
		snapshot.recordSource("runtime_diagnosis", err, false)
		c.progressf("warning=app runtime diagnosis unavailable: %v", err)
	} else {
		if runtimeDiagnosis == nil {
			snapshot.recordSource("runtime_diagnosis", errEvidenceUnavailable, false)
		} else {
			snapshot.recordSource("runtime_diagnosis", nil, false)
			if !strings.EqualFold(strings.TrimSpace(runtimeDiagnosis.Category), "available") {
				snapshot.Diagnosis = selectPrimaryOverviewDiagnosis(snapshot.Diagnosis, appDiagnosisToOverviewDiagnosis(runtimeDiagnosis))
			}
		}
	}
	return snapshot, nil
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
		pairs = append(pairs, kvPair{Key: "sync_now", Value: "fugue app release tracking sync " + shellSingleQuote(appRef)})
	}
	return writeKeyValues(w, pairs...)
}
