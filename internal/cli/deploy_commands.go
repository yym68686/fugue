package cli

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

type deployCommonOptions struct {
	Name            string
	Description     string
	EnvFile         string
	RuntimeName     string
	RuntimeID       string
	Replicas        int
	ServicePort     int
	Wait            bool
	SourceDir       string
	BuildStrategy   string
	DockerfilePath  string
	BuildContextDir string
}

type deployLocalOptions struct {
	deployCommonOptions
	AppRef        string
	AppID         string
	Dir           string
	RepoURLCompat string
	Branch        string
	Private       bool
	RepoToken     string
}

type deployGitHubOptions struct {
	deployCommonOptions
	Branch    string
	Private   bool
	RepoToken string
}

type deployImageOptions struct {
	Name        string
	Description string
	EnvFile     string
	RuntimeName string
	RuntimeID   string
	Replicas    int
	ServicePort int
	Wait        bool
}

type importBundle struct {
	PrimaryApp    model.App
	PrimaryOp     model.Operation
	Apps          []model.App
	Operations    []model.Operation
	ComposeStack  map[string]any
	FugueManifest map[string]any
}

type importBundleJSON struct {
	App           *model.App        `json:"app,omitempty"`
	Operation     *model.Operation  `json:"operation,omitempty"`
	Apps          []model.App       `json:"apps,omitempty"`
	Operations    []model.Operation `json:"operations,omitempty"`
	ComposeStack  map[string]any    `json:"compose_stack,omitempty"`
	FugueManifest map[string]any    `json:"fugue_manifest,omitempty"`
}

func runDeployWithStreams(args []string, stdout, stderr io.Writer) error {
	return runWithStreams(append([]string{"deploy"}, args...), stdout, stderr)
}

func (c *CLI) newDeployCommand() *cobra.Command {
	opts := deployLocalOptions{
		deployCommonOptions: deployCommonOptions{
			BuildStrategy: model.AppBuildStrategyAuto,
			Wait:          true,
		},
	}

	cmd := &cobra.Command{
		Use:     "deploy [path]",
		Aliases: []string{"up"},
		Short:   "Deploy local source, a GitHub repo, or a container image",
		Long: strings.TrimSpace(`
Deploy is the primary high-level entrypoint for Fugue.

Most users only need one API key plus a source location.

Without a subcommand it uploads local source from the current directory (or an
explicit path). Use "deploy github" to import from GitHub and "deploy image" to
create an app directly from an image reference.

Defaults:
  - Tenant is auto-selected when your key only sees one tenant.
  - Project defaults to "default" when omitted.
  - Runtime defaults to the shared managed runtime when omitted.
  - App name defaults from the directory, repo, or image name when possible.
`),
		Example: strings.TrimSpace(`
  export FUGUE_API_KEY=<your-api-key>
  fugue deploy .
  fugue deploy ./examples/demo --name demo
  fugue deploy --app demo .
  fugue deploy github owner/repo --branch main
  fugue deploy image nginx:1.27
`),
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(opts.RepoURLCompat) != "" {
				compat := deployGitHubOptions{
					deployCommonOptions: opts.deployCommonOptions,
					Branch:              opts.Branch,
					Private:             opts.Private,
					RepoToken:           opts.RepoToken,
				}
				baseDir, err := resolveDeployPath("", opts.Dir)
				if err != nil {
					return err
				}
				return c.runDeployGitHub(normalizeGitHubRepoArg(opts.RepoURLCompat), compat, baseDir)
			}
			pathArg := ""
			if len(args) == 1 {
				pathArg = args[0]
			}
			return c.runDeployLocal(pathArg, opts)
		},
	}

	bindCommonDeployFlags(cmd, &opts.deployCommonOptions, true)
	cmd.Flags().StringVar(&opts.AppRef, "app", "", "Update an existing app by name or ID")
	cmd.Flags().StringVar(&opts.Dir, "dir", "", "Project directory to upload")
	cmd.Flags().StringVar(&opts.AppID, "app-id", "", "Existing app ID to redeploy")
	cmd.Flags().StringVar(&opts.RepoURLCompat, "repo-url", "", "Compatibility flag for GitHub deploys; prefer 'deploy github'")
	cmd.Flags().StringVar(&opts.Branch, "branch", "", "Git branch for --repo-url compatibility mode")
	cmd.Flags().BoolVar(&opts.Private, "private", false, "Treat the repository as private")
	cmd.Flags().StringVar(&opts.RepoToken, "repo-token", "", "GitHub token for private repo imports")
	_ = cmd.Flags().MarkHidden("dir")
	_ = cmd.Flags().MarkHidden("app-id")
	_ = cmd.Flags().MarkHidden("repo-url")
	_ = cmd.Flags().MarkHidden("branch")
	_ = cmd.Flags().MarkHidden("private")
	_ = cmd.Flags().MarkHidden("repo-token")

	cmd.AddCommand(
		c.newDeployGitHubCommand(),
		c.newDeployImageCommand(),
		c.newDeployInspectCommand(),
		c.newDeployPlanCommand(),
	)
	return cmd
}

func (c *CLI) newDeployGitHubCommand() *cobra.Command {
	opts := deployGitHubOptions{
		deployCommonOptions: deployCommonOptions{
			BuildStrategy: model.AppBuildStrategyAuto,
			Wait:          true,
		},
	}
	cmd := &cobra.Command{
		Use:   "github <repo-or-url>",
		Short: "Deploy from GitHub",
		Long: strings.TrimSpace(`
Import a GitHub repository as an app.

You normally only need the repo reference. Fugue defaults the tenant, project,
runtime, and app name when they are not ambiguous.
`),
		Args: cobra.ExactArgs(1),
		Example: strings.TrimSpace(`
	  fugue deploy github owner/repo
  fugue deploy github owner/repo --branch main
  fugue deploy github https://github.com/example/app --private --repo-token $GITHUB_TOKEN
`),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.runDeployGitHub(normalizeGitHubRepoArg(args[0]), opts, "")
		},
	}
	bindCommonDeployFlags(cmd, &opts.deployCommonOptions, false)
	cmd.Flags().StringVar(&opts.Branch, "branch", "", "Git branch to import")
	cmd.Flags().BoolVar(&opts.Private, "private", false, "Treat the repository as private")
	cmd.Flags().StringVar(&opts.RepoToken, "repo-token", "", "GitHub token for private repo imports")
	return cmd
}

func (c *CLI) newDeployImageCommand() *cobra.Command {
	opts := deployImageOptions{Wait: true}
	cmd := &cobra.Command{
		Use:   "image <image-ref>",
		Short: "Deploy directly from an image reference",
		Long: strings.TrimSpace(`
Create an app directly from a container image.

If you omit --name, Fugue derives one from the image name. Tenant and project
selection follow the same automatic rules as "fugue deploy".
`),
		Args: cobra.ExactArgs(1),
		Example: strings.TrimSpace(`
	  fugue deploy image nginx:1.27
  fugue deploy image ghcr.io/example/app:latest --name demo --replicas 2
`),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.runDeployImage(args[0], opts)
		},
	}
	cmd.Flags().StringVar(&opts.Name, "name", "", "App name (defaults to the image name)")
	cmd.Flags().StringVar(&opts.Description, "description", "", "App description")
	cmd.Flags().StringVar(&opts.EnvFile, "env-file", "", "Local .env file to inject as app env")
	cmd.Flags().StringVar(&opts.RuntimeName, "runtime", "", "Runtime name")
	cmd.Flags().StringVar(&opts.RuntimeID, "runtime-id", "", "Runtime ID")
	cmd.Flags().IntVar(&opts.Replicas, "replicas", 0, "Desired replica count")
	cmd.Flags().IntVar(&opts.ServicePort, "port", 0, "Service port override")
	cmd.Flags().IntVar(&opts.ServicePort, "service-port", 0, "Service port override")
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for operation completion")
	_ = cmd.Flags().MarkHidden("runtime-id")
	_ = cmd.Flags().MarkHidden("service-port")
	return cmd
}

func bindCommonDeployFlags(cmd *cobra.Command, opts *deployCommonOptions, includeName bool) {
	if includeName {
		cmd.Flags().StringVar(&opts.Name, "name", "", "App name. Defaults from the source directory or repo name")
	}
	cmd.Flags().StringVar(&opts.Description, "description", "", "App description")
	cmd.Flags().StringVar(&opts.EnvFile, "env-file", "", "Local .env file to inject as app env")
	cmd.Flags().StringVar(&opts.RuntimeName, "runtime", "", "Runtime name. Defaults to the shared managed runtime")
	cmd.Flags().StringVar(&opts.RuntimeID, "runtime-id", "", "Runtime ID")
	cmd.Flags().IntVar(&opts.Replicas, "replicas", 0, "Desired replica count")
	cmd.Flags().IntVar(&opts.ServicePort, "port", 0, "Service port override")
	cmd.Flags().IntVar(&opts.ServicePort, "service-port", 0, "Service port override")
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for operation completion")
	cmd.Flags().StringVar(&opts.SourceDir, "source-dir", "", "Source directory relative to the project root")
	cmd.Flags().StringVar(&opts.BuildStrategy, "build", opts.BuildStrategy, "Override build detection: auto, static-site, dockerfile, buildpacks, nixpacks")
	cmd.Flags().StringVar(&opts.BuildStrategy, "build-strategy", opts.BuildStrategy, "Override build detection: auto, static-site, dockerfile, buildpacks, nixpacks")
	cmd.Flags().StringVar(&opts.DockerfilePath, "dockerfile", "", "Dockerfile path relative to the project root")
	cmd.Flags().StringVar(&opts.BuildContextDir, "context", "", "Docker build context relative to the project root")
	cmd.Flags().StringVar(&opts.DockerfilePath, "dockerfile-path", "", "Dockerfile path relative to the project root")
	cmd.Flags().StringVar(&opts.BuildContextDir, "build-context-dir", "", "Docker build context relative to the project root")
	_ = cmd.Flags().MarkHidden("runtime-id")
	_ = cmd.Flags().MarkHidden("service-port")
	_ = cmd.Flags().MarkHidden("build-strategy")
	_ = cmd.Flags().MarkHidden("dockerfile-path")
	_ = cmd.Flags().MarkHidden("build-context-dir")
}

func (c *CLI) runDeployLocal(pathArg string, opts deployLocalOptions) error {
	appRef := strings.TrimSpace(opts.AppRef)
	if strings.TrimSpace(opts.AppID) != "" {
		if appRef != "" && !strings.EqualFold(appRef, opts.AppID) {
			return fmt.Errorf("--app and --app-id must point to the same app")
		}
		appRef = strings.TrimSpace(opts.AppID)
	}
	if appRef != "" && strings.TrimSpace(opts.Name) != "" {
		return fmt.Errorf("--name cannot be used with --app")
	}

	workingDir, err := resolveDeployPath(pathArg, opts.Dir)
	if err != nil {
		return err
	}
	client, err := c.newClient()
	if err != nil {
		return err
	}

	runtimeID, err := resolveRuntimeSelection(client, opts.RuntimeID, opts.RuntimeName)
	if err != nil {
		return err
	}

	envVars, envPath, err := loadDeploymentEnv(workingDir, opts.EnvFile, false)
	if err != nil {
		return err
	}
	if envPath != "" {
		c.progressf("Loaded %d env vars from %s", len(envVars), envPath)
	}

	targetApp := model.App{}
	resolvedAppID := ""
	tenantID := ""
	projectSel := projectSelection{}
	projectLookupID := ""

	if appRef != "" {
		filterTenantID, filterProjectID, err := c.resolveFilterSelections(client)
		if err != nil {
			return err
		}
		targetApp, err = resolveAppReference(client, appRef, filterProjectID, filterTenantID)
		if err != nil {
			return err
		}
		resolvedAppID = targetApp.ID
	} else {
		tenantID, projectSel, projectLookupID, err = c.resolveCreateSelections(client)
		if err != nil {
			return err
		}
		if projectLookupID == "" && projectSel.Create == nil {
			projectLookupID, _ = resolveProjectReference(client, tenantID, "", "default")
		}
		opts.Name = strings.TrimSpace(opts.Name)
		if opts.Name == "" {
			opts.Name = defaultDeployAppName(workingDir, "")
		}
		if opts.Name == "" {
			opts.Name = "app"
		}
		resolvedAppID, err = resolveAppSelection(client, "", opts.Name, projectLookupID, tenantID)
		if err != nil {
			return err
		}
		if resolvedAppID != "" {
			targetApp, err = resolveAppReference(client, resolvedAppID, projectLookupID, tenantID)
			if err != nil {
				return err
			}
		}
	}

	archiveBaseName := strings.TrimSpace(opts.Name)
	if archiveBaseName == "" && strings.TrimSpace(targetApp.Name) != "" {
		archiveBaseName = targetApp.Name
	}
	if archiveBaseName == "" {
		archiveBaseName = defaultDeployAppName(workingDir, "")
	}
	if archiveBaseName == "" {
		archiveBaseName = "app"
	}

	archiveBytes, archiveName, err := createSourceArchive(workingDir, archiveBaseName)
	if err != nil {
		return err
	}

	request := importUploadRequest{
		AppID:           resolvedAppID,
		TenantID:        tenantID,
		SourceDir:       strings.TrimSpace(opts.SourceDir),
		Name:            strings.TrimSpace(opts.Name),
		Description:     strings.TrimSpace(opts.Description),
		BuildStrategy:   strings.TrimSpace(opts.BuildStrategy),
		RuntimeID:       strings.TrimSpace(runtimeID),
		Replicas:        opts.Replicas,
		ServicePort:     opts.ServicePort,
		DockerfilePath:  strings.TrimSpace(opts.DockerfilePath),
		BuildContextDir: strings.TrimSpace(opts.BuildContextDir),
		Env:             envVars,
	}
	if request.Name == "" && strings.TrimSpace(targetApp.Name) == "" {
		request.Name = archiveBaseName
	}
	if resolvedAppID == "" {
		if strings.TrimSpace(projectSel.ID) != "" {
			request.ProjectID = projectSel.ID
		} else if projectSel.Create != nil {
			request.Project = projectSel.Create
		}
	}

	c.progressf("Uploading %s (%d bytes)", archiveName, len(archiveBytes))
	response, err := client.ImportUpload(request, archiveName, archiveBytes)
	if err != nil {
		return err
	}
	return c.finishImportBundle(client, bundleFromUploadResponse(response), opts.Wait)
}

func (c *CLI) runDeployGitHub(repoURL string, opts deployGitHubOptions, workingDir string) error {
	if strings.TrimSpace(repoURL) == "" {
		return fmt.Errorf("repository is required")
	}
	var err error
	if strings.TrimSpace(workingDir) == "" {
		workingDir, err = os.Getwd()
		if err != nil {
			return fmt.Errorf("get working directory: %w", err)
		}
	}
	client, err := c.newClient()
	if err != nil {
		return err
	}
	tenantID, projectSel, _, err := c.resolveCreateSelections(client)
	if err != nil {
		return err
	}
	runtimeID, err := resolveRuntimeSelection(client, opts.RuntimeID, opts.RuntimeName)
	if err != nil {
		return err
	}
	envVars, envPath, err := loadDeploymentEnv(workingDir, opts.EnvFile, true)
	if err != nil {
		return err
	}
	if envPath != "" {
		c.progressf("Loaded %d env vars from %s", len(envVars), envPath)
	}

	name := strings.TrimSpace(opts.Name)
	if name == "" {
		name = defaultDeployAppName(workingDir, repoURL)
	}
	if name == "" {
		name = "app"
	}
	request := importGitHubRequest{
		TenantID:        tenantID,
		SourceDir:       strings.TrimSpace(opts.SourceDir),
		RepoURL:         repoURL,
		Branch:          strings.TrimSpace(opts.Branch),
		Name:            name,
		Description:     strings.TrimSpace(opts.Description),
		BuildStrategy:   strings.TrimSpace(opts.BuildStrategy),
		RuntimeID:       strings.TrimSpace(runtimeID),
		Replicas:        opts.Replicas,
		ServicePort:     opts.ServicePort,
		DockerfilePath:  strings.TrimSpace(opts.DockerfilePath),
		BuildContextDir: strings.TrimSpace(opts.BuildContextDir),
		Env:             envVars,
		RepoAuthToken:   strings.TrimSpace(opts.RepoToken),
	}
	if opts.Private {
		request.RepoVisibility = "private"
	}
	if strings.TrimSpace(projectSel.ID) != "" {
		request.ProjectID = projectSel.ID
	} else if projectSel.Create != nil {
		request.Project = projectSel.Create
	}

	c.progressf("Importing %s", request.RepoURL)
	response, err := client.ImportGitHub(request)
	if err != nil {
		return err
	}
	if response.RequestInProgress && response.App == nil && len(response.Apps) == 0 {
		if c.wantsJSON() {
			return writeJSON(c.stdout, response)
		}
		return writeKeyValues(c.stdout, kvPair{Key: "request_in_progress", Value: "true"})
	}
	return c.finishImportBundle(client, bundleFromGitHubResponse(response), opts.Wait)
}

func (c *CLI) runDeployImage(imageRef string, opts deployImageOptions) error {
	imageRef = strings.TrimSpace(imageRef)
	if imageRef == "" {
		return fmt.Errorf("image is required")
	}
	workingDir, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("get working directory: %w", err)
	}
	client, err := c.newClient()
	if err != nil {
		return err
	}
	tenantID, projectSel, _, err := c.resolveCreateSelections(client)
	if err != nil {
		return err
	}
	runtimeID, err := resolveRuntimeSelection(client, opts.RuntimeID, opts.RuntimeName)
	if err != nil {
		return err
	}
	envVars, envPath, err := loadDeploymentEnv(workingDir, opts.EnvFile, false)
	if err != nil {
		return err
	}
	if envPath != "" {
		c.progressf("Loaded %d env vars from %s", len(envVars), envPath)
	}

	name := strings.TrimSpace(opts.Name)
	if name == "" {
		name = imageRefAppName(imageRef)
	}
	if name == "" {
		name = "app"
	}
	request := importImageRequest{
		TenantID:    tenantID,
		ImageRef:    imageRef,
		Name:        name,
		Description: strings.TrimSpace(opts.Description),
		RuntimeID:   strings.TrimSpace(runtimeID),
		Replicas:    opts.Replicas,
		ServicePort: opts.ServicePort,
		Env:         envVars,
	}
	if strings.TrimSpace(projectSel.ID) != "" {
		request.ProjectID = projectSel.ID
	} else if projectSel.Create != nil {
		request.Project = projectSel.Create
	}

	c.progressf("Importing image %s", imageRef)
	response, err := client.ImportImage(request)
	if err != nil {
		return err
	}
	return c.finishImportBundle(client, importBundle{
		PrimaryApp: response.App,
		PrimaryOp:  response.Operation,
	}, opts.Wait)
}

func resolveDeployPath(pathArg, compatDir string) (string, error) {
	pathArg = strings.TrimSpace(pathArg)
	compatDir = strings.TrimSpace(compatDir)
	switch {
	case pathArg != "" && compatDir != "":
		return "", fmt.Errorf("path argument and --dir cannot be used together")
	case compatDir != "":
		pathArg = compatDir
	case pathArg == "":
		pathArg = "."
	}
	abs, err := filepath.Abs(pathArg)
	if err != nil {
		return "", fmt.Errorf("resolve path %s: %w", pathArg, err)
	}
	return abs, nil
}

func bundleFromGitHubResponse(response importGitHubResponse) importBundle {
	bundle := importBundle{
		Apps:          dedupeApps(response.Apps, response.App),
		Operations:    dedupeOperations(response.Operations, response.Operation),
		ComposeStack:  response.ComposeStack,
		FugueManifest: response.FugueManifest,
	}
	if response.App != nil {
		bundle.PrimaryApp = *response.App
	}
	if response.Operation != nil {
		bundle.PrimaryOp = *response.Operation
	}
	if bundle.PrimaryApp.ID == "" && len(bundle.Apps) > 0 {
		bundle.PrimaryApp = bundle.Apps[0]
	}
	if bundle.PrimaryOp.ID == "" && len(bundle.Operations) > 0 {
		bundle.PrimaryOp = bundle.Operations[0]
	}
	return bundle
}

func bundleFromUploadResponse(response importUploadResponse) importBundle {
	bundle := importBundle{
		Apps:          dedupeApps(response.Apps, response.App),
		Operations:    dedupeOperations(response.Operations, response.Operation),
		ComposeStack:  response.ComposeStack,
		FugueManifest: response.FugueManifest,
	}
	if response.App != nil {
		bundle.PrimaryApp = *response.App
	}
	if response.Operation != nil {
		bundle.PrimaryOp = *response.Operation
	}
	if bundle.PrimaryApp.ID == "" && len(bundle.Apps) > 0 {
		bundle.PrimaryApp = bundle.Apps[0]
	}
	if bundle.PrimaryOp.ID == "" && len(bundle.Operations) > 0 {
		bundle.PrimaryOp = bundle.Operations[0]
	}
	return bundle
}

func dedupeApps(apps []model.App, primary *model.App) []model.App {
	seen := map[string]struct{}{}
	out := make([]model.App, 0, len(apps)+1)
	if primary != nil && strings.TrimSpace(primary.ID) != "" {
		out = append(out, *primary)
		seen[primary.ID] = struct{}{}
	}
	for _, app := range apps {
		if strings.TrimSpace(app.ID) == "" {
			continue
		}
		if _, ok := seen[app.ID]; ok {
			continue
		}
		seen[app.ID] = struct{}{}
		out = append(out, app)
	}
	return out
}

func dedupeOperations(operations []model.Operation, primary *model.Operation) []model.Operation {
	seen := map[string]struct{}{}
	out := make([]model.Operation, 0, len(operations)+1)
	if primary != nil && strings.TrimSpace(primary.ID) != "" {
		out = append(out, *primary)
		seen[primary.ID] = struct{}{}
	}
	for _, op := range operations {
		if strings.TrimSpace(op.ID) == "" {
			continue
		}
		if _, ok := seen[op.ID]; ok {
			continue
		}
		seen[op.ID] = struct{}{}
		out = append(out, op)
	}
	return out
}

func (c *CLI) finishImportBundle(client *Client, bundle importBundle, wait bool) error {
	if bundle.PrimaryApp.ID != "" && bundle.PrimaryOp.ID != "" {
		c.progressf("Queued operation %s for app %s", bundle.PrimaryOp.ID, bundle.PrimaryApp.ID)
	} else if len(bundle.Operations) > 0 {
		c.progressf("Queued %d operation(s)", len(bundle.Operations))
	}

	if !wait || len(bundle.Operations) == 0 {
		return c.renderImportBundle(bundle, false)
	}

	finalOps, err := c.waitForOperations(client, bundle.Operations)
	if err != nil {
		return err
	}
	finalApps, err := fetchFinalApps(client, bundle.Apps, finalOps)
	if err != nil {
		return err
	}
	bundle.Operations = finalOps
	if op, ok := findOperationByID(finalOps, bundle.PrimaryOp.ID); ok {
		bundle.PrimaryOp = op
	}
	bundle.Apps = finalApps
	if app, ok := findAppByID(finalApps, bundle.PrimaryApp.ID); ok {
		bundle.PrimaryApp = app
	} else if len(finalApps) > 0 {
		bundle.PrimaryApp = finalApps[0]
	}
	return c.renderImportBundle(bundle, true)
}

func (c *CLI) renderImportBundle(bundle importBundle, waited bool) error {
	if c.wantsJSON() {
		payload := importBundleJSON{
			Apps:          bundle.Apps,
			Operations:    bundle.Operations,
			ComposeStack:  bundle.ComposeStack,
			FugueManifest: bundle.FugueManifest,
		}
		if strings.TrimSpace(bundle.PrimaryApp.ID) != "" {
			appCopy := bundle.PrimaryApp
			payload.App = &appCopy
		}
		if strings.TrimSpace(bundle.PrimaryOp.ID) != "" {
			opCopy := bundle.PrimaryOp
			payload.Operation = &opCopy
		}
		return writeJSON(c.stdout, payload)
	}

	pairs := make([]kvPair, 0, 4)
	if strings.TrimSpace(bundle.PrimaryApp.ID) != "" {
		pairs = append(pairs, kvPair{Key: "app_id", Value: bundle.PrimaryApp.ID})
	}
	if strings.TrimSpace(bundle.PrimaryOp.ID) != "" {
		pairs = append(pairs, kvPair{Key: "operation_id", Value: bundle.PrimaryOp.ID})
	}
	if waited && bundle.PrimaryApp.Route != nil && strings.TrimSpace(bundle.PrimaryApp.Route.PublicURL) != "" {
		pairs = append(pairs, kvPair{Key: "url", Value: bundle.PrimaryApp.Route.PublicURL})
	}
	if err := writeKeyValues(c.stdout, pairs...); err != nil {
		return err
	}
	if len(bundle.Apps) > 1 {
		if _, err := fmt.Fprintln(c.stdout); err != nil {
			return err
		}
		return writeMultiAppSummary(c.stdout, bundle.Apps)
	}
	return nil
}

func (c *CLI) waitForOperations(client *Client, operations []model.Operation) ([]model.Operation, error) {
	order := make([]string, 0, len(operations))
	pending := make(map[string]model.Operation, len(operations))
	for _, op := range operations {
		if strings.TrimSpace(op.ID) == "" {
			continue
		}
		if _, ok := pending[op.ID]; ok {
			continue
		}
		order = append(order, op.ID)
		pending[op.ID] = op
	}
	if len(pending) == 0 {
		return nil, nil
	}

	lastStatus := make(map[string]string, len(pending))
	final := make(map[string]model.Operation, len(pending))
	for len(pending) > 0 {
		for _, id := range order {
			base, ok := pending[id]
			if !ok {
				continue
			}
			current, err := client.GetOperation(id)
			if err != nil {
				return nil, err
			}
			status := strings.TrimSpace(current.Status)
			if status != lastStatus[id] {
				if len(order) == 1 {
					c.progressf("operation_status=%s", status)
				} else {
					c.progressf("operation_id=%s operation_status=%s", current.ID, status)
				}
				lastStatus[id] = status
			}

			switch status {
			case model.OperationStatusCompleted:
				final[id] = current
				delete(pending, id)
			case model.OperationStatusFailed:
				return nil, c.operationFailure(client, current)
			default:
				pending[id] = base
			}
		}
		if len(pending) == 0 {
			break
		}
		time.Sleep(2 * time.Second)
	}

	out := make([]model.Operation, 0, len(order))
	for _, id := range order {
		if op, ok := final[id]; ok {
			out = append(out, op)
		}
	}
	return out, nil
}

func (c *CLI) operationFailure(client *Client, op model.Operation) error {
	if strings.TrimSpace(op.AppID) != "" {
		logs, err := client.GetBuildLogs(op.AppID, op.ID, 200)
		if err == nil {
			text := strings.TrimSpace(logs.Logs)
			if text == "" {
				text = strings.TrimSpace(logs.Summary)
			}
			if text != "" {
				if strings.TrimSpace(op.ErrorMessage) != "" {
					return fmt.Errorf("operation %s failed: %s\n\n%s", op.ID, strings.TrimSpace(op.ErrorMessage), text)
				}
				return fmt.Errorf("operation %s failed\n\n%s", op.ID, text)
			}
		}
	}
	if strings.TrimSpace(op.ErrorMessage) != "" {
		return fmt.Errorf("operation %s failed: %s", op.ID, strings.TrimSpace(op.ErrorMessage))
	}
	if strings.TrimSpace(op.ResultMessage) != "" {
		return fmt.Errorf("operation %s failed: %s", op.ID, strings.TrimSpace(op.ResultMessage))
	}
	return fmt.Errorf("operation %s failed", op.ID)
}

func fetchFinalApps(client *Client, apps []model.App, operations []model.Operation) ([]model.App, error) {
	order := make([]string, 0, len(apps)+len(operations))
	seen := make(map[string]struct{}, len(apps)+len(operations))
	for _, app := range apps {
		if strings.TrimSpace(app.ID) == "" {
			continue
		}
		if _, ok := seen[app.ID]; ok {
			continue
		}
		seen[app.ID] = struct{}{}
		order = append(order, app.ID)
	}
	for _, op := range operations {
		if strings.TrimSpace(op.AppID) == "" {
			continue
		}
		if _, ok := seen[op.AppID]; ok {
			continue
		}
		seen[op.AppID] = struct{}{}
		order = append(order, op.AppID)
	}

	finalApps := make([]model.App, 0, len(order))
	for _, appID := range order {
		app, err := client.GetApp(appID)
		if err != nil {
			return nil, err
		}
		finalApps = append(finalApps, app)
	}
	return finalApps, nil
}

func findAppByID(apps []model.App, id string) (model.App, bool) {
	for _, app := range apps {
		if strings.EqualFold(app.ID, strings.TrimSpace(id)) {
			return app, true
		}
	}
	return model.App{}, false
}

func findOperationByID(operations []model.Operation, id string) (model.Operation, bool) {
	for _, op := range operations {
		if strings.EqualFold(op.ID, strings.TrimSpace(id)) {
			return op, true
		}
	}
	return model.Operation{}, false
}
