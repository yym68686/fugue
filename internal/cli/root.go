package cli

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

type rootOptions struct {
	BaseURL     string
	Token       string
	TenantID    string
	TenantName  string
	ProjectID   string
	ProjectName string
	Output      string
	JSONOutput  bool
}

type CLI struct {
	stdout io.Writer
	stderr io.Writer
	root   rootOptions
}

func Run(args []string) error {
	return runWithStreams(args, os.Stdout, os.Stderr)
}

func runWithStreams(args []string, stdout, stderr io.Writer) error {
	cli := newCLI(stdout, stderr)
	cmd := cli.newRootCommand()
	cmd.SetOut(stdout)
	cmd.SetErr(stderr)
	cmd.SetArgs(args)
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	return cmd.Execute()
}

func newCLI(stdout, stderr io.Writer) *CLI {
	return &CLI{
		stdout: stdout,
		stderr: stderr,
		root: rootOptions{
			Output: "text",
		},
	}
}

func (c *CLI) newRootCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "fugue",
		Short: "Semantic CLI for deploying and managing Fugue apps",
		Long: strings.TrimSpace(`
Fugue is a semantic CLI over the Fugue control-plane API.

Use name-based commands such as "deploy", "app list", and "app logs" instead of
calling low-level API endpoints directly. The CLI resolves tenant, project, app,
runtime, domain, and workspace names where possible, while still allowing
ID-based flags as hidden escape hatches for compatibility.

Environment variables:
  FUGUE_BASE_URL
  FUGUE_TOKEN / FUGUE_API_KEY / FUGUE_BOOTSTRAP_KEY
  FUGUE_TENANT / FUGUE_TENANT_NAME / FUGUE_TENANT_ID
  FUGUE_PROJECT / FUGUE_PROJECT_NAME / FUGUE_PROJECT_ID
`),
		Example: strings.TrimSpace(`
  fugue deploy .
  fugue deploy github owner/repo --branch main
  fugue deploy image nginx:1.27
  fugue app list
  fugue app status my-app
  fugue app logs my-app
  fugue env list my-app
  fugue domain add my-app www.example.com
  fugue workspace list my-app
`),
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			return c.validateOutput()
		},
	}

	flags := cmd.PersistentFlags()
	flags.StringVar(&c.root.BaseURL, "base-url", c.root.BaseURL, "Fugue API base URL (or FUGUE_BASE_URL)")
	flags.StringVar(&c.root.Token, "token", c.root.Token, "Fugue API token (or FUGUE_TOKEN/FUGUE_API_KEY/FUGUE_BOOTSTRAP_KEY)")
	flags.StringVar(&c.root.TenantName, "tenant", c.root.TenantName, "Tenant name or slug")
	flags.StringVar(&c.root.ProjectName, "project", c.root.ProjectName, "Project name")
	flags.StringVarP(&c.root.Output, "output", "o", c.root.Output, "Output format: text or json")
	flags.BoolVar(&c.root.JSONOutput, "json", false, "Shortcut for --output json")
	flags.StringVar(&c.root.TenantID, "tenant-id", c.root.TenantID, "Tenant ID")
	flags.StringVar(&c.root.ProjectID, "project-id", c.root.ProjectID, "Project ID")
	_ = flags.MarkHidden("tenant-id")
	_ = flags.MarkHidden("project-id")

	cmd.AddCommand(
		c.newDeployCommand(),
		c.newAppCommand(),
		c.newEnvCommand(),
		c.newDomainCommand(),
		c.newWorkspaceCommand(),
	)
	return cmd
}

func (c *CLI) effectiveOutput() string {
	if c.root.JSONOutput {
		return "json"
	}
	if strings.TrimSpace(c.root.Output) == "" {
		return "text"
	}
	return strings.TrimSpace(strings.ToLower(c.root.Output))
}

func (c *CLI) validateOutput() error {
	switch c.effectiveOutput() {
	case "text", "json":
		c.root.Output = c.effectiveOutput()
		return nil
	default:
		return fmt.Errorf("unsupported output format %q", c.root.Output)
	}
}

func (c *CLI) wantsJSON() bool {
	return c.effectiveOutput() == "json"
}

func (c *CLI) newClient() (*Client, error) {
	if err := c.validateOutput(); err != nil {
		return nil, err
	}
	return NewClient(c.effectiveBaseURL(), c.effectiveToken())
}

func (c *CLI) progressf(format string, args ...any) {
	if c.wantsJSON() {
		return
	}
	_, _ = fmt.Fprintf(c.stderr, format+"\n", args...)
}

func (c *CLI) resolveFilterSelections(client *Client) (string, string, error) {
	tenantIDValue := c.effectiveTenantID()
	tenantNameValue := c.effectiveTenantName()
	projectIDValue := c.effectiveProjectID()
	projectNameValue := c.effectiveProjectName()

	needsTenant := strings.TrimSpace(tenantIDValue) != "" ||
		strings.TrimSpace(tenantNameValue) != "" ||
		strings.TrimSpace(projectIDValue) != "" ||
		strings.TrimSpace(projectNameValue) != ""

	tenantID := ""
	var err error
	if needsTenant {
		tenantID, err = resolveTenantSelection(client, tenantIDValue, tenantNameValue)
		if err != nil {
			return "", "", err
		}
	}

	projectID, err := resolveProjectReference(client, tenantID, projectIDValue, projectNameValue)
	if err != nil {
		return "", "", err
	}
	return tenantID, projectID, nil
}

func (c *CLI) resolveCreateSelections(client *Client) (string, projectSelection, string, error) {
	tenantID, err := resolveTenantSelection(client, c.effectiveTenantID(), c.effectiveTenantName())
	if err != nil {
		return "", projectSelection{}, "", err
	}

	projectSel, err := resolveProjectCreationSelection(client, tenantID, c.effectiveProjectID(), c.effectiveProjectName())
	if err != nil {
		return "", projectSelection{}, "", err
	}

	return tenantID, projectSel, strings.TrimSpace(projectSel.ID), nil
}

func (c *CLI) effectiveBaseURL() string {
	return firstNonEmpty(c.root.BaseURL, os.Getenv("FUGUE_BASE_URL"))
}

func (c *CLI) effectiveToken() string {
	return firstNonEmpty(c.root.Token, os.Getenv("FUGUE_TOKEN"), os.Getenv("FUGUE_API_KEY"), os.Getenv("FUGUE_BOOTSTRAP_KEY"))
}

func (c *CLI) effectiveTenantID() string {
	return firstNonEmpty(c.root.TenantID, os.Getenv("FUGUE_TENANT_ID"))
}

func (c *CLI) effectiveTenantName() string {
	return firstNonEmpty(c.root.TenantName, os.Getenv("FUGUE_TENANT"), os.Getenv("FUGUE_TENANT_NAME"))
}

func (c *CLI) effectiveProjectID() string {
	return firstNonEmpty(c.root.ProjectID, os.Getenv("FUGUE_PROJECT_ID"))
}

func (c *CLI) effectiveProjectName() string {
	return firstNonEmpty(c.root.ProjectName, os.Getenv("FUGUE_PROJECT"), os.Getenv("FUGUE_PROJECT_NAME"))
}
