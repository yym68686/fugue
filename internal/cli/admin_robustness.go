package cli

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

func (c *CLI) newAdminRobustnessCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "robustness",
		Short: "Inspect self-healing robustness checks and incidents",
	}
	cmd.AddCommand(
		c.newAdminRobustnessStatusCommand(),
		c.newAdminRobustnessCheckCommand(),
		c.newAdminRobustnessIncidentsCommand(),
		c.newAdminRobustnessRepairPlanCommand(),
		c.newAdminRobustnessRepairCommand(),
	)
	return cmd
}

func (c *CLI) newAdminRobustnessStatusCommand() *cobra.Command {
	opts := struct{ Subject string }{}
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show server-side robustness status",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			status, err := client.GetRobustnessStatus(opts.Subject)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"status": status})
			}
			return writeRobustnessStatus(c.stdout, status)
		},
	}
	cmd.Flags().StringVar(&opts.Subject, "subject", "", "Optional hostname, app, node, or subsystem to include in the check")
	return cmd
}

func (c *CLI) newAdminRobustnessCheckCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "check <subject>",
		Short: "Run robustness checks scoped to a hostname or subject",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.runAdminRobustnessCheck(args[0])
		},
	}
	cmd.AddCommand(
		c.newAdminRobustnessTypedCheckCommand("node", "Run robustness checks scoped to one node", "<node-name>"),
		c.newAdminRobustnessTypedCheckCommand("service", "Run robustness checks scoped to one hostname or app", "<hostname-or-app>"),
		c.newAdminRobustnessTypedCheckCommand("edge", "Run robustness checks scoped to one edge node", "<edge-id>"),
	)
	return cmd
}

func (c *CLI) newAdminRobustnessTypedCheckCommand(kind, short, argName string) *cobra.Command {
	return &cobra.Command{
		Use:   kind + " " + argName,
		Short: short,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.runAdminRobustnessCheck(args[0])
		},
	}
}

func (c *CLI) runAdminRobustnessCheck(subject string) error {
	client, err := c.newClient()
	if err != nil {
		return err
	}
	status, err := client.CheckRobustnessSubject(subject)
	if err != nil {
		return err
	}
	if c.wantsJSON() {
		return writeJSON(c.stdout, map[string]any{"status": status})
	}
	return writeRobustnessStatus(c.stdout, status)
}

func (c *CLI) newAdminRobustnessIncidentsCommand() *cobra.Command {
	opts := struct{ Subject string }{}
	cmd := &cobra.Command{
		Use:     "incidents",
		Aliases: []string{"incident"},
		Short:   "List or inspect current robustness incidents",
	}
	list := &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List current robustness incidents",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			incidents, err := client.ListRobustnessIncidents(opts.Subject)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"incidents": incidents})
			}
			return writeRobustnessIncidentTable(c.stdout, incidents)
		},
	}
	show := &cobra.Command{
		Use:   "show <incident-id>",
		Short: "Show one current robustness incident",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			incident, _, err := client.GetRobustnessIncident(args[0], opts.Subject)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"incident": incident})
			}
			return writeRobustnessIncident(c.stdout, incident)
		},
	}
	list.Flags().StringVar(&opts.Subject, "subject", "", "Optional subject scope")
	show.Flags().StringVar(&opts.Subject, "subject", "", "Optional subject scope")
	cmd.AddCommand(list, show)
	return cmd
}

func (c *CLI) newAdminRobustnessRepairPlanCommand() *cobra.Command {
	opts := struct{ Subject string }{}
	cmd := &cobra.Command{
		Use:   "repair-plan <incident-id>",
		Short: "Plan a robustness repair without changing state",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			plan, err := client.PlanRobustnessRepair(args[0], opts.Subject)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"plan": plan})
			}
			return writeRobustnessRepairPlan(c.stdout, plan)
		},
	}
	cmd.Flags().StringVar(&opts.Subject, "subject", "", "Optional subject scope")
	return cmd
}

func (c *CLI) newAdminRobustnessRepairCommand() *cobra.Command {
	opts := struct {
		Subject string
		DryRun  bool
	}{DryRun: true}
	cmd := &cobra.Command{
		Use:   "repair <incident-id>",
		Short: "Run a robustness repair when an automatic safe action exists",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			plan, err := client.RunRobustnessRepair(args[0], opts.Subject, model.RobustnessRepairRequest{DryRun: opts.DryRun})
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"plan": plan})
			}
			return writeRobustnessRepairPlan(c.stdout, plan)
		},
	}
	cmd.Flags().StringVar(&opts.Subject, "subject", "", "Optional subject scope")
	cmd.Flags().BoolVar(&opts.DryRun, "dry-run", true, "Only plan the repair; non-dry-run repairs require server-side safe automation")
	return cmd
}

func writeRobustnessStatus(w io.Writer, status model.RobustnessStatus) error {
	if err := writeKeyValues(w,
		kvPair{Key: "pass", Value: fmt.Sprintf("%t", status.Pass)},
		kvPair{Key: "block_rollout", Value: fmt.Sprintf("%t", status.BlockRollout)},
		kvPair{Key: "subject", Value: firstNonEmpty(status.Subject, "-")},
		kvPair{Key: "checks", Value: fmt.Sprintf("%d", len(status.Checks))},
		kvPair{Key: "incidents", Value: fmt.Sprintf("%d", len(status.Incidents))},
		kvPair{Key: "generated_at", Value: formatTime(status.GeneratedAt)},
	); err != nil {
		return err
	}
	if len(status.Incidents) > 0 {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if err := writeRobustnessIncidentTable(w, status.Incidents); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	return writeRobustnessCheckTable(w, status.Checks)
}

func writeRobustnessCheckTable(w io.Writer, checks []model.RobustnessCheck) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "SEVERITY\tPASS\tSUBJECT\tCHECK\tOBSERVED\tMESSAGE"); err != nil {
		return err
	}
	for _, check := range checks {
		if _, err := fmt.Fprintf(tw, "%s\t%t\t%s\t%s\t%s\t%s\n",
			check.Severity,
			check.Pass,
			firstNonEmpty(check.Subject, "-"),
			check.Name,
			firstNonEmpty(check.Observed, "-"),
			firstNonEmpty(check.Message, "-"),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeRobustnessIncidentTable(w io.Writer, incidents []model.RobustnessIncident) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "ID\tSEVERITY\tSTATUS\tSUBJECT\tCHECK\tMESSAGE"); err != nil {
		return err
	}
	for _, incident := range incidents {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n",
			incident.ID,
			incident.Severity,
			incident.Status,
			firstNonEmpty(incident.Subject, "-"),
			incident.CheckName,
			firstNonEmpty(incident.Message, incident.Observed, "-"),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeRobustnessIncident(w io.Writer, incident model.RobustnessIncident) error {
	return writeKeyValues(w,
		kvPair{Key: "id", Value: incident.ID},
		kvPair{Key: "severity", Value: incident.Severity},
		kvPair{Key: "status", Value: incident.Status},
		kvPair{Key: "subject", Value: incident.Subject},
		kvPair{Key: "check", Value: incident.CheckName},
		kvPair{Key: "title", Value: incident.Title},
		kvPair{Key: "expected", Value: firstNonEmpty(incident.Expected, "-")},
		kvPair{Key: "observed", Value: firstNonEmpty(incident.Observed, "-")},
		kvPair{Key: "message", Value: firstNonEmpty(incident.Message, "-")},
		kvPair{Key: "repair_hint", Value: firstNonEmpty(incident.RepairHint, "-")},
	)
}

func writeRobustnessRepairPlan(w io.Writer, plan model.RobustnessRepairPlan) error {
	if err := writeKeyValues(w,
		kvPair{Key: "incident_id", Value: plan.IncidentID},
		kvPair{Key: "status", Value: plan.Status},
		kvPair{Key: "safe", Value: fmt.Sprintf("%t", plan.Safe)},
		kvPair{Key: "dry_run", Value: fmt.Sprintf("%t", plan.DryRun)},
		kvPair{Key: "message", Value: firstNonEmpty(plan.Message, "-")},
		kvPair{Key: "generated_at", Value: formatTime(plan.GeneratedAt)},
	); err != nil {
		return err
	}
	if len(plan.Actions) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "KIND\tSUBJECT\tAUTOMATIC\tRISK\tDESCRIPTION\tCOMMAND"); err != nil {
		return err
	}
	for _, action := range plan.Actions {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%t\t%s\t%s\t%s\n",
			action.Kind,
			firstNonEmpty(action.Subject, "-"),
			action.Automatic,
			firstNonEmpty(action.Risk, "-"),
			strings.TrimSpace(action.Description),
			firstNonEmpty(action.Command, "-"),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}
