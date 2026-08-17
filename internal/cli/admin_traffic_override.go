package cli

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

func (c *CLI) newAdminTrafficOverrideCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "traffic-override",
		Aliases: []string{"traffic-overrides"},
		Short:   "Manage independently signed emergency DNS traffic overrides",
	}
	cmd.AddCommand(
		c.newAdminTrafficOverrideListCommand(),
		c.newAdminTrafficOverrideGetCommand(),
		c.newAdminTrafficOverrideStageCommand(),
		c.newAdminTrafficOverrideRevokeCommand(),
		c.newAdminTrafficOverrideSigningKeyCommand(),
	)
	return cmd
}

func (c *CLI) newAdminTrafficOverrideListCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List staged and revoked traffic overrides",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			overrides, err := client.ListTrafficOverrides()
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"overrides": overrides})
			}
			return writeTrafficOverrideTable(c.stdout, overrides)
		},
	}
}

func (c *CLI) newAdminTrafficOverrideGetCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "get <hostname>",
		Short: "Show one signed traffic override",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			override, err := client.GetTrafficOverride(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"override": override})
			}
			return writeTrafficOverride(c.stdout, override)
		},
	}
}

func (c *CLI) newAdminTrafficOverrideStageCommand() *cobra.Command {
	opts := struct {
		Answers            []string
		RequiredHostRoutes []string
		RouteGeneration    string
		RouteDigest        string
		ExpiresIn          time.Duration
		Reason             string
		ExpectedGeneration uint64
	}{ExpiresIn: 15 * time.Minute}
	cmd := &cobra.Command{
		Use:   "stage <hostname>",
		Short: "Stage a signed override without activating DNS serving",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.ExpiresIn <= 0 || opts.ExpiresIn > 24*time.Hour {
				return fmt.Errorf("--expires-in must be greater than zero and at most 24h")
			}
			hostname := strings.TrimSpace(args[0])
			routes := append([]string{}, opts.RequiredHostRoutes...)
			if len(routes) == 0 {
				routes = []string{hostname}
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			override, err := client.PutTrafficOverride(hostname, trafficOverridePutRequest{
				Answers:            opts.Answers,
				RequiredHostRoutes: routes,
				RouteGeneration:    opts.RouteGeneration,
				RouteDigest:        opts.RouteDigest,
				ExpiresAt:          time.Now().UTC().Add(opts.ExpiresIn),
				Reason:             opts.Reason,
				ExpectedGeneration: opts.ExpectedGeneration,
			})
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"override": override})
			}
			return writeTrafficOverride(c.stdout, override)
		},
	}
	cmd.Flags().StringSliceVar(&opts.Answers, "answer", nil, "Emergency A/AAAA answer IP; repeat for multiple answers")
	cmd.Flags().StringSliceVar(&opts.RequiredHostRoutes, "required-host-route", nil, "Host route that must already be loaded; defaults to hostname")
	cmd.Flags().StringVar(&opts.RouteGeneration, "route-generation", "", "Observed Edge route generation")
	cmd.Flags().StringVar(&opts.RouteDigest, "route-digest", "", "Observed Edge route digest (sha256:...)")
	cmd.Flags().DurationVar(&opts.ExpiresIn, "expires-in", opts.ExpiresIn, "Bounded override lifetime, at most 24h")
	cmd.Flags().StringVar(&opts.Reason, "reason", "", "Auditable emergency reason")
	cmd.Flags().Uint64Var(&opts.ExpectedGeneration, "expected-generation", 0, "Current generation for CAS; zero creates a new override")
	_ = cmd.MarkFlagRequired("answer")
	_ = cmd.MarkFlagRequired("route-generation")
	_ = cmd.MarkFlagRequired("route-digest")
	_ = cmd.MarkFlagRequired("reason")
	return cmd
}

func (c *CLI) newAdminTrafficOverrideRevokeCommand() *cobra.Command {
	opts := struct {
		Reason             string
		ExpectedGeneration uint64
	}{}
	cmd := &cobra.Command{
		Use:   "revoke <hostname>",
		Short: "Write a signed override revocation with CAS",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			override, err := client.RevokeTrafficOverride(args[0], opts.Reason, opts.ExpectedGeneration)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"override": override})
			}
			return writeTrafficOverride(c.stdout, override)
		},
	}
	cmd.Flags().StringVar(&opts.Reason, "reason", "", "Auditable revocation reason")
	cmd.Flags().Uint64Var(&opts.ExpectedGeneration, "expected-generation", 0, "Current generation for CAS")
	_ = cmd.MarkFlagRequired("reason")
	_ = cmd.MarkFlagRequired("expected-generation")
	return cmd
}

func (c *CLI) newAdminTrafficOverrideSigningKeyCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "signing-key", Short: "Inspect or rotate override signing key metadata"}
	cmd.AddCommand(c.newAdminTrafficOverrideSigningKeyStatusCommand(), c.newAdminTrafficOverrideSigningKeyRotateCommand())
	return cmd
}

func (c *CLI) newAdminTrafficOverrideSigningKeyStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show signing key IDs without secret material",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			status, err := client.GetTrafficOverrideSigningKey()
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"signing_key": status})
			}
			return writeTrafficOverrideSigningKey(c.stdout, status)
		},
	}
}

func (c *CLI) newAdminTrafficOverrideSigningKeyRotateCommand() *cobra.Command {
	var expected uint64
	cmd := &cobra.Command{
		Use:   "rotate",
		Short: "Rotate the signing key and retain one previous verifier key",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			status, err := client.RotateTrafficOverrideSigningKey(expected)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"signing_key": status})
			}
			return writeTrafficOverrideSigningKey(c.stdout, status)
		},
	}
	cmd.Flags().Uint64Var(&expected, "expected-generation", 0, "Current signing key generation for CAS")
	_ = cmd.MarkFlagRequired("expected-generation")
	return cmd
}

func writeTrafficOverride(w io.Writer, override model.TrafficOverride) error {
	return writeKeyValues(w,
		kvPair{Key: "hostname", Value: override.Hostname},
		kvPair{Key: "generation", Value: fmt.Sprintf("%d", override.Generation)},
		kvPair{Key: "state", Value: override.State},
		kvPair{Key: "answers", Value: strings.Join(override.Answers, ", ")},
		kvPair{Key: "required_host_routes", Value: strings.Join(override.RequiredHostRoutes, ", ")},
		kvPair{Key: "route_generation", Value: override.RouteGeneration},
		kvPair{Key: "route_digest", Value: override.RouteDigest},
		kvPair{Key: "artifact_digest", Value: override.ArtifactDigest},
		kvPair{Key: "key_id", Value: override.KeyID},
		kvPair{Key: "expires_at", Value: formatTime(override.ExpiresAt)},
		kvPair{Key: "operator", Value: override.Operator},
		kvPair{Key: "reason", Value: override.Reason},
	)
}

func writeTrafficOverrideTable(w io.Writer, overrides []model.TrafficOverride) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "HOSTNAME\tGENERATION\tSTATE\tANSWERS\tROUTE_GENERATION\tKEY_ID\tEXPIRES_AT\tREASON"); err != nil {
		return err
	}
	for _, override := range overrides {
		if _, err := fmt.Fprintf(tw, "%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\n", override.Hostname, override.Generation, override.State, strings.Join(override.Answers, ","), override.RouteGeneration, override.KeyID, formatTime(override.ExpiresAt), override.Reason); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeTrafficOverrideSigningKey(w io.Writer, status model.TrafficOverrideSigningKeyStatus) error {
	return writeKeyValues(w,
		kvPair{Key: "schema", Value: status.Schema},
		kvPair{Key: "generation", Value: fmt.Sprintf("%d", status.Generation)},
		kvPair{Key: "current_key_id", Value: status.CurrentKeyID},
		kvPair{Key: "current_public_key", Value: status.CurrentPublicKey},
		kvPair{Key: "previous_key_id", Value: firstNonEmpty(status.PreviousKeyID, "-")},
		kvPair{Key: "previous_public_key", Value: firstNonEmpty(status.PreviousPublicKey, "-")},
		kvPair{Key: "created_at", Value: formatTime(status.CreatedAt)},
		kvPair{Key: "rotated_at", Value: formatTime(status.RotatedAt)},
		kvPair{Key: "updated_at", Value: formatTime(status.UpdatedAt)},
	)
}
