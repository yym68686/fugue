package cli

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os/signal"
	"syscall"
	"time"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

type appOverviewSnapshot struct {
	App             model.App                  `json:"app"`
	Domains         []model.AppDomain          `json:"domains,omitempty"`
	Bindings        []model.ServiceBinding     `json:"bindings,omitempty"`
	BackingServices []model.BackingService     `json:"backing_services,omitempty"`
	Operations      []model.Operation          `json:"operations,omitempty"`
	Images          *appImageInventoryResponse `json:"images,omitempty"`
}

func (c *CLI) newAppOverviewCommand() *cobra.Command {
	opts := struct {
		ShowSecrets bool
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
			return c.renderAppOverviewSnapshot(client, snapshot, false, opts.ShowSecrets)
		},
	}
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
		c.progressf("warning=domain inventory unavailable: %v", err)
	} else {
		snapshot.Domains = domains
	}
	if bindings, err := client.ListAppBindings(app.ID); err != nil {
		c.progressf("warning=service binding inventory unavailable: %v", err)
	} else {
		snapshot.Bindings = bindings.Bindings
		snapshot.BackingServices = bindings.BackingServices
	}
	if operations, err := client.ListOperations(app.ID); err != nil {
		c.progressf("warning=operation inventory unavailable: %v", err)
	} else {
		snapshot.Operations = operations
	}
	if images, err := client.GetAppImages(app.ID); err != nil {
		c.progressf("warning=image inventory unavailable: %v", err)
	} else {
		snapshot.Images = &images
	}
	return snapshot, nil
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
		return writeJSON(c.stdout, snapshot)
	}
	if _, err := fmt.Fprintf(c.stdout, "observed_at=%s\n", formatTime(time.Now().UTC())); err != nil {
		return err
	}
	if err := c.renderAppStatus(client, snapshot.App); err != nil {
		return err
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
