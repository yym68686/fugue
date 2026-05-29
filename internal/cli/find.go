package cli

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"fugue/internal/model"
)

func (c *CLI) newFindCommand() *cobra.Command {
	var types []string
	var limit int
	cmd := &cobra.Command{
		Use:     "find <query>",
		Aliases: []string{"search"},
		Short:   "Search visible tenants, projects, apps, domains, services, runtimes, and operations",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			response, err := client.SearchResources(args[0], normalizeFindTypes(types), limit)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return writeFindResponse(c.stdout, response)
		},
	}
	cmd.Flags().StringArrayVar(&types, "type", nil, "Restrict resource kind; repeat or pass comma-separated values")
	cmd.Flags().IntVar(&limit, "limit", 50, "Maximum results to return")
	return cmd
}

func normalizeFindTypes(types []string) []string {
	out := []string{}
	seen := map[string]bool{}
	for _, raw := range types {
		for _, value := range strings.Split(raw, ",") {
			value = strings.ToLower(strings.TrimSpace(value))
			if value == "" || seen[value] {
				continue
			}
			seen[value] = true
			out = append(out, value)
		}
	}
	return out
}

func writeFindResponse(w io.Writer, response model.SearchResponse) error {
	if len(response.Results) == 0 {
		_, err := fmt.Fprintf(w, "No resources matched %q.\n", response.Query)
		return err
	}
	if err := writeFindResultTable(w, response.Results); err != nil {
		return err
	}
	commands := findFollowupCommands(response.Results)
	if len(commands) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "next_commands"); err != nil {
		return err
	}
	for _, command := range commands {
		if _, err := fmt.Fprintln(w, "  "+command); err != nil {
			return err
		}
	}
	return nil
}

func writeFindResultTable(w io.Writer, results []model.SearchResult) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "KIND\tNAME\tTENANT\tPROJECT\tAPP\tSTATUS\tURL\tINTERNAL_URL\tMATCH"); err != nil {
		return err
	}
	for _, result := range results {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			result.Kind,
			firstNonEmptyTrimmed(result.Name, result.ID),
			firstNonEmptyTrimmed(result.TenantName, result.TenantID, "-"),
			firstNonEmptyTrimmed(result.ProjectName, result.ProjectID, "-"),
			firstNonEmptyTrimmed(result.AppName, result.AppID, "-"),
			firstNonEmptyTrimmed(result.Status, "-"),
			firstNonEmptyTrimmed(result.PublicURL, "-"),
			firstNonEmptyTrimmed(result.InternalURL, "-"),
			strings.Join(result.MatchedFields, ","),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func findFollowupCommands(results []model.SearchResult) []string {
	commands := []string{}
	seen := map[string]bool{}
	for _, result := range results {
		for _, command := range findFollowupCommandsForResult(result) {
			if command == "" || seen[command] {
				continue
			}
			seen[command] = true
			commands = append(commands, command)
		}
		if len(commands) >= 8 {
			return commands
		}
	}
	return commands
}

func findFollowupCommandsForResult(result model.SearchResult) []string {
	tenantRef := firstNonEmptyTrimmed(result.TenantName, result.TenantID)
	projectRef := firstNonEmptyTrimmed(result.ProjectName, result.ProjectID)
	appRef := firstNonEmptyTrimmed(result.AppName, result.AppID)
	withTenantProject := func(base string) string {
		parts := []string{"fugue"}
		if tenantRef != "" {
			parts = append(parts, "--tenant", shellSingleQuote(tenantRef))
		}
		if projectRef != "" {
			parts = append(parts, "--project", shellSingleQuote(projectRef))
		}
		parts = append(parts, base)
		return strings.Join(parts, " ")
	}

	switch result.Kind {
	case "app", "domain", "operation":
		if appRef == "" {
			return nil
		}
		return []string{withTenantProject("app overview " + shellSingleQuote(appRef))}
	case "project":
		if projectRef == "" {
			return nil
		}
		return []string{withTenantProject("project overview " + shellSingleQuote(projectRef))}
	case "service":
		if appRef != "" {
			return []string{withTenantProject("app overview " + shellSingleQuote(appRef))}
		}
		if projectRef != "" {
			return []string{withTenantProject("project overview " + shellSingleQuote(projectRef))}
		}
	case "tenant":
		if tenantRef != "" {
			return []string{"fugue --tenant " + shellSingleQuote(tenantRef) + " project ls"}
		}
	}
	return nil
}
