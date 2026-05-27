package cli

import (
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"

	"fugue/internal/model"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

func (c *CLI) newProjectRoutesCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "routes",
		Aliases: []string{"route", "entrypoints"},
		Short:   "Inspect and replace the project route table",
	}
	cmd.AddCommand(
		c.newProjectRoutesShowCommand(),
		c.newProjectRoutesApplyCommand(),
		c.newProjectRoutesDeleteCommand(),
	)
	return cmd
}

func (c *CLI) newProjectRoutesShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "show <project>",
		Aliases: []string{"get", "ls", "list"},
		Short:   "Show project domains, entrypoints, and compiled routes",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			project, err := c.resolveNamedProject(client, args[0])
			if err != nil {
				return err
			}
			response, err := client.GetProjectRouteTable(project.ID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return renderProjectRouteTable(c.stdout, response.RouteTable, c.showIDs())
		},
	}
}

func (c *CLI) newProjectRoutesApplyCommand() *cobra.Command {
	opts := struct {
		File string
	}{}
	cmd := &cobra.Command{
		Use:   "apply <project>",
		Short: "Replace the project route table from a fugue.yaml-style file",
		Example: strings.TrimSpace(`
fugue project routes apply uni-api-web --file fugue.yaml
fugue project routes apply uni-api-web --file routes.yaml
`),
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(opts.File) == "" {
				return fmt.Errorf("--file is required")
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			project, err := c.resolveNamedProject(client, args[0])
			if err != nil {
				return err
			}
			table, err := readProjectRouteTableFile(opts.File)
			if err != nil {
				return err
			}
			response, err := client.PutProjectRouteTable(project.ID, table)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return renderProjectRouteTable(c.stdout, response.RouteTable, c.showIDs())
		},
	}
	cmd.Flags().StringVarP(&opts.File, "file", "f", "", "YAML or JSON file containing domains and entrypoints")
	return cmd
}

func (c *CLI) newProjectRoutesDeleteCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "delete <project>",
		Aliases: []string{"clear", "remove", "rm"},
		Short:   "Delete the explicit project route table and fall back to legacy app routes",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			project, err := c.resolveNamedProject(client, args[0])
			if err != nil {
				return err
			}
			response, err := client.DeleteProjectRouteTable(project.ID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			if _, err := fmt.Fprintf(c.stdout, "deleted=%t\n", response.Deleted); err != nil {
				return err
			}
			return renderProjectRouteTable(c.stdout, response.RouteTable, c.showIDs())
		},
	}
}

type projectRouteTableFile struct {
	RouteTable  *projectRouteTableBody       `json:"route_table" yaml:"route_table"`
	Domains     []projectRouteDomainFile     `json:"domains" yaml:"domains"`
	Entrypoints []projectRouteEntrypointFile `json:"entrypoints" yaml:"entrypoints"`
}

type projectRouteTableBody struct {
	Domains     []projectRouteDomainFile     `json:"domains" yaml:"domains"`
	Entrypoints []projectRouteEntrypointFile `json:"entrypoints" yaml:"entrypoints"`
}

type projectRouteDomainFile struct {
	Name         string `json:"name" yaml:"name"`
	Hostname     string `json:"hostname" yaml:"hostname"`
	Host         string `json:"host" yaml:"host"`
	TLS          string `json:"tls" yaml:"tls"`
	OwnerService string `json:"owner_service" yaml:"owner_service"`
	OwnerAppID   string `json:"owner_app_id" yaml:"owner_app_id"`
}

type projectRouteEntrypointFile struct {
	Name   string                            `json:"name" yaml:"name"`
	Domain string                            `json:"domain" yaml:"domain"`
	Routes []projectRouteEntrypointRouteFile `json:"routes" yaml:"routes"`
}

type projectRouteEntrypointRouteFile struct {
	Path        string `json:"path" yaml:"path"`
	PathPrefix  string `json:"path_prefix" yaml:"path_prefix"`
	Service     string `json:"service" yaml:"service"`
	AppID       string `json:"app_id" yaml:"app_id"`
	StripPrefix bool   `json:"strip_prefix" yaml:"strip_prefix"`
	Rewrite     string `json:"rewrite" yaml:"rewrite"`
}

func readProjectRouteTableFile(path string) (model.ProjectRouteTable, error) {
	path = strings.TrimSpace(path)
	var data []byte
	var err error
	if path == "-" {
		data, err = io.ReadAll(os.Stdin)
	} else {
		data, err = os.ReadFile(path)
	}
	if err != nil {
		return model.ProjectRouteTable{}, err
	}
	var file projectRouteTableFile
	if err := yaml.Unmarshal(data, &file); err != nil {
		return model.ProjectRouteTable{}, err
	}
	domains := file.Domains
	entrypoints := file.Entrypoints
	if file.RouteTable != nil {
		domains = file.RouteTable.Domains
		entrypoints = file.RouteTable.Entrypoints
	}
	return model.NormalizeProjectRouteTable(model.ProjectRouteTable{
		Domains:     convertProjectRouteDomains(domains),
		Entrypoints: convertProjectRouteEntrypoints(entrypoints),
	}), nil
}

func convertProjectRouteDomains(items []projectRouteDomainFile) []model.ProjectRouteDomain {
	out := make([]model.ProjectRouteDomain, 0, len(items))
	for _, item := range items {
		out = append(out, model.ProjectRouteDomain{
			Name:         item.Name,
			Hostname:     item.Hostname,
			Host:         item.Host,
			TLS:          item.TLS,
			OwnerService: item.OwnerService,
			OwnerAppID:   item.OwnerAppID,
		})
	}
	return out
}

func convertProjectRouteEntrypoints(items []projectRouteEntrypointFile) []model.ProjectRouteEntrypoint {
	out := make([]model.ProjectRouteEntrypoint, 0, len(items))
	for _, item := range items {
		routes := make([]model.ProjectRouteEntrypointRoute, 0, len(item.Routes))
		for _, route := range item.Routes {
			routes = append(routes, model.ProjectRouteEntrypointRoute{
				Path:        route.Path,
				PathPrefix:  route.PathPrefix,
				Service:     route.Service,
				AppID:       route.AppID,
				StripPrefix: route.StripPrefix,
				Rewrite:     route.Rewrite,
			})
		}
		out = append(out, model.ProjectRouteEntrypoint{
			Name:   item.Name,
			Domain: item.Domain,
			Routes: routes,
		})
	}
	return out
}

func renderProjectRouteTable(w io.Writer, table model.ProjectRouteTable, showIDs bool) error {
	if err := writeKeyValues(w,
		kvPair{Key: "project_id", Value: table.ProjectID},
		kvPair{Key: "tenant_id", Value: table.TenantID},
		kvPair{Key: "legacy", Value: fmt.Sprintf("%t", table.Legacy)},
		kvPair{Key: "domains", Value: fmt.Sprintf("%d", len(table.Domains))},
		kvPair{Key: "entrypoints", Value: fmt.Sprintf("%d", len(table.Entrypoints))},
		kvPair{Key: "routes", Value: fmt.Sprintf("%d", len(table.Bindings))},
	); err != nil {
		return err
	}
	if len(table.Bindings) == 0 {
		_, err := fmt.Fprintln(w, "No project routes.")
		return err
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if showIDs {
		if _, err := fmt.Fprintln(tw, "HOSTNAME\tPATH\tSERVICE\tAPP ID\tENTRYPOINT\tURL"); err != nil {
			return err
		}
	} else if _, err := fmt.Fprintln(tw, "HOSTNAME\tPATH\tSERVICE\tENTRYPOINT\tURL"); err != nil {
		return err
	}
	for _, binding := range table.Bindings {
		entrypoint := strings.TrimSpace(binding.EntrypointName)
		if entrypoint == "" {
			entrypoint = "-"
		}
		if showIDs {
			if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n", binding.Hostname, binding.PathPrefix, firstNonEmpty(binding.Service, binding.AppName), binding.AppID, entrypoint, binding.PublicURL); err != nil {
				return err
			}
			continue
		}
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", binding.Hostname, binding.PathPrefix, firstNonEmpty(binding.Service, binding.AppName), entrypoint, binding.PublicURL); err != nil {
			return err
		}
	}
	return tw.Flush()
}
