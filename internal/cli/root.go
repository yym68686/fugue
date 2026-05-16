package cli

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

const defaultCloudBaseURL = "https://api.fugue.pro"

const (
	rootVersionBuildCommitAnnotation = "fugue.buildCommit"
	rootVersionBuildTimeAnnotation   = "fugue.buildTime"
	rootVersionTemplate              = `version={{.Version}}
commit={{index .Annotations "` + rootVersionBuildCommitAnnotation + `"}}
built_at={{index .Annotations "` + rootVersionBuildTimeAnnotation + `"}}
`
)

type rootOptions struct {
	BaseURL     string
	WebBaseURL  string
	Token       string
	TenantID    string
	TenantName  string
	ProjectID   string
	ProjectName string
	Output      string
	JSONOutput  bool
	ShowIDs     bool
	OutputFile  string
	Redact      bool
	ConfirmRaw  bool
}

type CLI struct {
	stdout      io.Writer
	stderr      io.Writer
	root        rootOptions
	observer    requestObserver
	outputFile  *os.File
	outputReady bool
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
	defer cli.closeOutputFile()
	return cmd.Execute()
}

func newCLI(stdout, stderr io.Writer) *CLI {
	return &CLI{
		stdout: stdout,
		stderr: stderr,
		root: rootOptions{
			Output: "text",
			Redact: true,
		},
	}
}

func (c *CLI) newRootCommand() *cobra.Command {
	buildInfo := currentCLIBuildInfo()
	cmd := &cobra.Command{
		Use:     "fugue",
		Short:   "Semantic CLI for deploying and managing Fugue apps",
		Version: buildInfo.Version,
		Annotations: map[string]string{
			rootVersionBuildCommitAnnotation: buildInfo.Commit,
			rootVersionBuildTimeAnnotation:   buildInfo.BuiltAt,
		},
		Long: strings.TrimSpace(`
Fugue is a semantic CLI over the Fugue control-plane API.

Quick start for most users:
  1. Install the CLI:
     macOS / Linux:
       curl -fsSL https://raw.githubusercontent.com/yym68686/fugue/main/scripts/install_fugue_cli.sh | sh
     Windows PowerShell:
       powershell -NoProfile -ExecutionPolicy Bypass -Command "irm https://raw.githubusercontent.com/yym68686/fugue/main/scripts/install_fugue_cli.ps1 | iex"
  2. Open the Access keys page and copy one key:
     Fugue Cloud: https://fugue.pro/app/api-keys
     Self-hosted: your Fugue web URL + /app/api-keys (for example https://app.example.com/app/api-keys)
     Use a tenant API key for normal deploys. Use a platform-admin/bootstrap key only for admin commands.
  3. Export the key and run normal commands:
     export FUGUE_API_KEY=<copied-access-key>
     fugue deploy .
     fugue app ls

	Defaults and auto-selection:
	  - Base URL defaults to FUGUE_BASE_URL, then FUGUE_API_URL, then ` + defaultCloudBaseURL + `.
	  - Web Base URL defaults to FUGUE_WEB_BASE_URL, then APP_BASE_URL, then a best-effort guess from the API base URL.
	  - Tenant is auto-selected when your key only sees one tenant.
	  - Deploy and create flows default to the "default" project when you do not pass --project.
	  - App and operation JSON output redacts secrets by default. Pass --show-secrets only when you explicitly need raw values.
	  - Pass --json as a shortcut for --output json, and use --output-file to mirror stdout into a local file.
	  - Diagnostic commands redact sensitive values by default. Pass --redact=false together with --confirm-raw-output only when you explicitly need unredacted evidence.
	  - Prefer names. Use --show-ids when you need internal identifiers in text output.
	  - ID flags stay hidden as compatibility escape hatches.

Use name-based commands such as "deploy", "app ls", "operation ls", and
"app logs" instead of calling low-level API endpoints directly. The CLI
resolves tenant, project, app, runtime, service, domain, binding, and
workspace names where possible.

Environment variables:
  FUGUE_API_KEY / FUGUE_TOKEN / FUGUE_BOOTSTRAP_KEY
  FUGUE_BASE_URL / FUGUE_API_URL
  FUGUE_WEB_BASE_URL / APP_BASE_URL
  FUGUE_TENANT / FUGUE_TENANT_NAME / FUGUE_TENANT_ID
  FUGUE_PROJECT / FUGUE_PROJECT_NAME / FUGUE_PROJECT_ID
  FUGUE_SKIP_UPDATE_CHECK
`),
		Example: strings.TrimSpace(`
	  curl -fsSL https://raw.githubusercontent.com/yym68686/fugue/main/scripts/install_fugue_cli.sh | sh
	  export FUGUE_API_KEY=<copied-access-key>
	  fugue deploy .
	  fugue app ls
	  fugue version --check-latest
	  fugue upgrade
	  # self-hosted control plane
	  export FUGUE_BASE_URL=https://api.example.com
	  export FUGUE_WEB_BASE_URL=https://app.example.com
	  fugue --tenant acme deploy github owner/repo
	  # in Codex: "Use fugue CLI and the current FUGUE_API_KEY to deploy this project."
	  fugue --project marketing app logs web --follow
	  fugue --base-url https://api.example.com app ls
	  fugue deploy inspect .
	  fugue deploy github owner/repo --branch main
	  fugue deploy github owner/repo --service-env-file gateway=.env.gateway --service-env-file runtime=.env.runtime
	  fugue deploy image nginx:1.27
	  fugue tenant ls
	  fugue app create my-app --github owner/repo --branch main
	  fugue app status my-app
	  fugue app overview my-app
	  fugue app source show my-app
	  fugue source-upload show upload_123
	  fugue app build my-app
	  fugue app deploy my-app
	  fugue app failover policy set my-app --app-to runtime-b
	  fugue app logs runtime my-app --follow
	  fugue app logs build my-app --operation op_import_123
	  fugue app service attach my-app postgres
	  fugue app command set my-app --command "python app.py"
	  fugue app config put my-app /app/config.yaml --from-file config.yaml
	  fugue app storage set my-app --size 10Gi --mount /data
	  fugue app db configure my-app --database app --user app
	  fugue app db query my-app --sql "select count(*) from users"
	  fugue app logs table my-app --table gateway_request_logs --since 1h --match status=500
	  fugue app logs pods my-app
	  fugue app request my-app /healthz
	  fugue app diagnose my-app
	  fugue app domain primary set my-app www.example.com
	  fugue service ls
	  fugue service postgres create app-db --runtime shared
	  fugue operation ls --app my-app
	  fugue operation ls --project marketing --type deploy --status pending
	  fugue operation show op_123 --show-secrets
	  fugue runtime enroll create edge-a
	  fugue runtime doctor shared
	  fugue admin runtime access show shared
	  fugue admin node-updater task ls --status pending
	  fugue admin discovery bundle show
	  fugue project overview
	  fugue project watch marketing
	  fugue project edit marketing --description "landing pages"
	  fugue project images usage marketing
	  fugue admin cluster status
	  fugue admin cluster node-policy status
	  fugue admin cluster pods --namespace kube-system
	  fugue admin cluster node inspect gcp1
	  fugue admin cluster workload show kube-system deployment coredns
	  fugue admin cluster dns resolve api.github.com --server 10.43.0.10
	  fugue admin cluster net connect api.github.com:443
	  fugue admin cluster net websocket my-app --path /ws
	  fugue admin cluster tls probe 104.18.32.47:443 --server-name api.github.com
	  fugue api request GET /v1/apps
	  fugue workflow run ./signup.yaml --json
	  fugue diagnose fs my-app --path /workspace/data --json
	  fugue logs collect my-app --request-id req_123 --since 30m --json
	  fugue logs query my-app --request-id req_123 --since 30m --status 200 --json
	  fugue debug bundle my-app --request-id req_123 --archive ./bundle.zip --json
	  fugue diagnose timing -- app overview my-app
	  fugue admin users ls
	  fugue admin users resolve user@example.com
	  fugue web diagnose admin-users
	`),
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if err := c.validateOutput(); err != nil {
				return err
			}
			if err := c.configureOutputWriter(cmd); err != nil {
				return err
			}
			if err := c.validateRedactionMode(); err != nil {
				return err
			}
			c.maybeWarnAboutCLIUpdate(cmd)
			return nil
		},
	}
	cmd.SetVersionTemplate(rootVersionTemplate)
	cmd.Flags().Bool("version", false, "Show the Fugue CLI build version")

	flags := cmd.PersistentFlags()
	flags.StringVar(&c.root.BaseURL, "base-url", c.root.BaseURL, "Optional API base URL. Defaults to FUGUE_BASE_URL, then FUGUE_API_URL, then "+defaultCloudBaseURL)
	flags.StringVar(&c.root.WebBaseURL, "web-base-url", c.root.WebBaseURL, "Optional web base URL. Defaults to FUGUE_WEB_BASE_URL, then APP_BASE_URL, then a best-effort guess from --base-url")
	flags.StringVar(&c.root.Token, "token", c.root.Token, "API key or bootstrap key. Reads FUGUE_API_KEY, FUGUE_TOKEN, or FUGUE_BOOTSTRAP_KEY")
	flags.StringVar(&c.root.TenantName, "tenant", c.root.TenantName, "Optional tenant name or slug. Needed only when your key can see multiple tenants")
	flags.StringVar(&c.root.ProjectName, "project", c.root.ProjectName, "Optional project name. Deploy/create defaults to the default project when omitted")
	flags.StringVarP(&c.root.Output, "output", "o", c.root.Output, "Output format: text or json")
	flags.BoolVar(&c.root.JSONOutput, "json", false, "Shortcut for --output json")
	flags.BoolVar(&c.root.ShowIDs, "show-ids", false, "Include internal IDs in text output where supported")
	flags.StringVar(&c.root.OutputFile, "output-file", c.root.OutputFile, "Also write stdout output to a local file")
	flags.BoolVar(&c.root.Redact, "redact", c.root.Redact, "Redact sensitive values in diagnostic output (pass --redact=false for raw output)")
	flags.BoolVar(&c.root.ConfirmRaw, "confirm-raw-output", false, "Required together with --redact=false to allow unredacted output")
	flags.StringVar(&c.root.TenantID, "tenant-id", c.root.TenantID, "Tenant ID")
	flags.StringVar(&c.root.ProjectID, "project-id", c.root.ProjectID, "Project ID")
	_ = flags.MarkHidden("tenant-id")
	_ = flags.MarkHidden("project-id")

	cmd.AddCommand(
		c.newDeployCommand(),
		c.newAppCommand(),
		c.newWorkflowCommand(),
		c.newLogsCommand(),
		c.newDebugCommand(),
		c.newSourceUploadCommand(),
		c.newTenantCommand(),
		c.newProjectCommand(),
		c.newRuntimeCommand(),
		c.newServiceCommand(),
		c.newVersionCommand(),
		c.newUpgradeCommand(),
		c.newAPICommand(),
		c.newDiagnoseCommand(),
		c.newWebCommand(),
		c.newOpsCommand(),
		hideCompatCommand(c.newTemplateCommand(), "fugue deploy inspect"),
		c.newAdminCommand(),
		hideCompatCommand(c.newCurlCommand(), "fugue api request"),
		c.newEnvCompatCommand(),
		c.newFilesCompatCommand(),
		c.newDomainCompatCommand(),
		c.newWorkspaceCompatCommand(),
	)
	applyHelpDocs(cmd)
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

func (c *CLI) shouldRedact() bool {
	return c.root.Redact
}

func (c *CLI) showIDs() bool {
	return !c.wantsJSON() && c.root.ShowIDs
}

func (c *CLI) validateRedactionMode() error {
	if c.root.Redact || c.root.ConfirmRaw {
		return nil
	}
	return fmt.Errorf("refusing unredacted output without --confirm-raw-output")
}

func (c *CLI) configureOutputWriter(cmd *cobra.Command) error {
	if c.outputReady {
		return nil
	}
	c.outputReady = true
	filePath := strings.TrimSpace(c.root.OutputFile)
	if filePath == "" {
		return nil
	}
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("open output file %s: %w", filePath, err)
	}
	c.outputFile = file
	writer := io.MultiWriter(c.stdout, file)
	c.stdout = writer
	cmd.SetOut(writer)
	return nil
}

func (c *CLI) closeOutputFile() {
	if c == nil || c.outputFile == nil {
		return
	}
	_ = c.outputFile.Close()
	c.outputFile = nil
}

func (c *CLI) newClient() (*Client, error) {
	if err := c.validateOutput(); err != nil {
		return nil, err
	}
	return newClientWithOptions(c.effectiveBaseURL(), c.effectiveToken(), clientOptions{
		Observer:     c.observer,
		RequireToken: true,
	})
}

func (c *CLI) newWebClient(cookie string) (*Client, error) {
	if err := c.validateOutput(); err != nil {
		return nil, err
	}
	baseURL := c.effectiveWebBaseURL()
	if strings.TrimSpace(baseURL) == "" {
		return nil, fmt.Errorf("web base url is required; pass --web-base-url or set FUGUE_WEB_BASE_URL/APP_BASE_URL")
	}
	return newClientWithOptions(baseURL, c.effectiveToken(), clientOptions{
		Cookie:       cookie,
		Observer:     c.observer,
		RequireToken: false,
	})
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
	return firstNonEmpty(c.root.BaseURL, os.Getenv("FUGUE_BASE_URL"), os.Getenv("FUGUE_API_URL"), defaultCloudBaseURL)
}

func (c *CLI) effectiveWebBaseURL() string {
	return firstNonEmpty(c.root.WebBaseURL, os.Getenv("FUGUE_WEB_BASE_URL"), os.Getenv("APP_BASE_URL"), deriveWebBaseURL(c.effectiveBaseURL()))
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

func deriveWebBaseURL(apiBaseURL string) string {
	apiBaseURL = strings.TrimSpace(apiBaseURL)
	if apiBaseURL == "" {
		return ""
	}
	parsed, err := url.Parse(apiBaseURL)
	if err != nil {
		return ""
	}
	if strings.HasPrefix(parsed.Hostname(), "api.") {
		hostname := strings.TrimPrefix(parsed.Hostname(), "api.")
		if hostname == "" {
			return ""
		}
		if port := parsed.Port(); port != "" {
			parsed.Host = hostname + ":" + port
		} else {
			parsed.Host = hostname
		}
		parsed.Path = ""
		parsed.RawPath = ""
		parsed.RawQuery = ""
		parsed.Fragment = ""
		return strings.TrimRight(parsed.String(), "/")
	}
	if parsed.Path == "" || parsed.Path == "/" {
		return strings.TrimRight(parsed.String(), "/")
	}
	parsed.Path = ""
	parsed.RawPath = ""
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return strings.TrimRight(parsed.String(), "/")
}
