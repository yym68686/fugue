package cli

import (
	"fmt"
	"os"
	"strings"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

type appCreateOptions struct {
	deployCommonOptions
	ImageRef  string
	RepoURL   string
	Private   bool
	RepoToken string
	Branch    string
}

func (c *CLI) newAppCreateCommand() *cobra.Command {
	opts := appCreateOptions{
		deployCommonOptions: deployCommonOptions{
			BuildStrategy: model.AppBuildStrategyAuto,
			Replicas:      1,
		},
	}
	cmd := &cobra.Command{
		Use:   "create <name>",
		Short: "Create an app record and staged source definition",
		Long: strings.TrimSpace(`
Create is the staged workflow entrypoint.

Use "deploy" when you want Fugue to import and deploy immediately. Use "app create"
when you want to register the app first, then run "app rebuild" when you are ready
to prepare the first release artifact.
`),
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
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
			files, err := buildDeployFiles(workingDir, opts.FileSpecs, opts.SecretFileSpecs)
			if err != nil {
				return err
			}
			persistentStorage, err := buildDeployPersistentStorage(workingDir, opts.StorageSize, opts.StorageClass, opts.StorageMounts, opts.StorageFiles)
			if err != nil {
				return err
			}
			postgres, err := c.buildDeployManagedPostgres(client, args[0], opts.deployCommonOptions)
			if err != nil {
				return err
			}
			source, err := buildCreateAppSource(opts)
			if err != nil {
				return err
			}
			if source == nil {
				return fmt.Errorf("one of --github or --image is required")
			}

			spec := model.AppSpec{
				Env:               envVars,
				Replicas:          opts.Replicas,
				RuntimeID:         strings.TrimSpace(runtimeID),
				Files:             files,
				PersistentStorage: persistentStorage,
				Postgres:          postgres,
				NetworkMode:       deployNetworkMode(opts.Background),
			}
			if port := appCreateServicePort(*source, opts.ServicePort, opts.Background); port > 0 {
				spec.Ports = []int{port}
			}
			applyCLIStartupCommand(&spec, opts.StartupCommand)
			model.ApplyAppSpecDefaults(&spec)

			request := createAppRequest{
				TenantID:    tenantID,
				Name:        strings.TrimSpace(args[0]),
				Description: strings.TrimSpace(opts.Description),
				Spec:        spec,
				Source:      source,
			}
			if strings.TrimSpace(projectSel.ID) != "" {
				request.ProjectID = projectSel.ID
			} else if projectSel.Create != nil {
				return fmt.Errorf("app create does not support creating a project implicitly; create the project first or pass an existing --project")
			}

			app, err := client.CreateApp(request)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{
					"app":       app,
					"next_step": fmt.Sprintf("fugue app rebuild %s", app.Name),
				})
			}
			return c.renderAppCreateResult(app)
		},
	}
	bindAppCreateFlags(cmd, &opts)
	return cmd
}

func bindAppCreateFlags(cmd *cobra.Command, opts *appCreateOptions) {
	cmd.Flags().StringVar(&opts.Description, "description", "", "App description")
	cmd.Flags().StringVar(&opts.ImageRef, "image", "", "Container image reference to stage for later rebuild")
	cmd.Flags().StringVar(&opts.RepoURL, "github", "", "GitHub repo URL or owner/repo to stage for later rebuild")
	cmd.Flags().BoolVar(&opts.Private, "private", false, "Treat the staged GitHub source as a private repo")
	cmd.Flags().StringVar(&opts.RepoToken, "repo-token", "", "Repository auth token for private GitHub sources")
	cmd.Flags().StringVar(&opts.Branch, "branch", "", "GitHub branch to stage")
	cmd.Flags().StringVar(&opts.EnvFile, "env-file", "", "Local .env file to inject as app env")
	cmd.Flags().StringVar(&opts.RuntimeName, "runtime", "", "Runtime name. Defaults to the shared managed runtime")
	cmd.Flags().StringVar(&opts.RuntimeID, "runtime-id", "", "Runtime ID")
	cmd.Flags().IntVar(&opts.Replicas, "replicas", opts.Replicas, "Desired replica count after deploy")
	cmd.Flags().IntVar(&opts.ServicePort, "port", 0, "Service port override")
	cmd.Flags().StringVar(&opts.SourceDir, "source-dir", "", "Source directory relative to the repo or upload root")
	cmd.Flags().StringVar(&opts.BuildStrategy, "build", opts.BuildStrategy, "Override build detection: auto, static-site, dockerfile, buildpacks, nixpacks")
	cmd.Flags().StringVar(&opts.DockerfilePath, "dockerfile", "", "Dockerfile path relative to the source root")
	cmd.Flags().StringVar(&opts.BuildContextDir, "context", "", "Docker build context relative to the source root")
	cmd.Flags().BoolVar(&opts.Background, "background", false, "Create the app as a background worker with no public ingress")
	cmd.Flags().StringVar(&opts.StartupCommand, "command", "", "Startup shell command override")
	cmd.Flags().StringArrayVar(&opts.FileSpecs, "file", nil, "Declarative app file from a local source: <absolute-path>[:mode]=<local-file>")
	cmd.Flags().StringArrayVar(&opts.SecretFileSpecs, "secret-file", nil, "Secret declarative app file from a local source: <absolute-path>[:mode]=<local-file>")
	cmd.Flags().StringVar(&opts.StorageSize, "storage-size", "", "Persistent storage size, for example 10Gi")
	cmd.Flags().StringVar(&opts.StorageClass, "storage-class", "", "Persistent storage class")
	cmd.Flags().StringArrayVar(&opts.StorageMounts, "mount", nil, "Persistent directory mount path, for example /data")
	cmd.Flags().StringArrayVar(&opts.StorageFiles, "mount-file", nil, "Persistent file mount from a local source: <absolute-path>[:mode]=<local-file>")
	cmd.Flags().BoolVar(&opts.ManagedPostgres, "managed-postgres", false, "Provision an app-owned managed Postgres database")
	cmd.Flags().StringVar(&opts.PostgresRuntime, "postgres-runtime", "", "Runtime name for managed Postgres")
	cmd.Flags().StringVar(&opts.PostgresRuntimeID, "postgres-runtime-id", "", "Runtime ID for managed Postgres")
	cmd.Flags().StringVar(&opts.PostgresDatabase, "postgres-database", "", "Database name for managed Postgres")
	cmd.Flags().StringVar(&opts.PostgresUser, "postgres-user", "", "Database user for managed Postgres")
	cmd.Flags().StringVar(&opts.PostgresPassword, "postgres-password", "", "Database password for managed Postgres. Generates a random password when omitted")
	cmd.Flags().StringVar(&opts.PostgresImage, "postgres-image", "", "Managed Postgres image override")
	cmd.Flags().StringVar(&opts.PostgresServiceName, "postgres-service-name", "", "Managed Postgres service name override")
	cmd.Flags().StringVar(&opts.PostgresStorageSize, "postgres-storage-size", "", "Managed Postgres storage size")
	cmd.Flags().StringVar(&opts.PostgresStorageClass, "postgres-storage-class", "", "Managed Postgres storage class")
	cmd.Flags().IntVar(&opts.PostgresInstances, "postgres-instances", 0, "Managed Postgres instance count")
	cmd.Flags().IntVar(&opts.PostgresSyncReplicas, "postgres-sync-replicas", 0, "Managed Postgres synchronous replica count")
	cmd.Flags().StringVar(&opts.PostgresFailoverTo, "postgres-failover-to", "", "Runtime name for managed Postgres failover")
	cmd.Flags().StringVar(&opts.PostgresFailoverRuntimeID, "postgres-failover-runtime-id", "", "Runtime ID for managed Postgres failover")
	_ = cmd.Flags().MarkHidden("runtime-id")
	_ = cmd.Flags().MarkHidden("postgres-runtime-id")
	_ = cmd.Flags().MarkHidden("postgres-failover-runtime-id")
}

func buildCreateAppSource(opts appCreateOptions) (*model.AppSource, error) {
	hasImage := strings.TrimSpace(opts.ImageRef) != ""
	hasGitHub := strings.TrimSpace(opts.RepoURL) != ""
	switch {
	case hasImage && hasGitHub:
		return nil, fmt.Errorf("--image and --github cannot be used together")
	case hasImage:
		source := model.AppSource{
			Type:     model.AppSourceTypeDockerImage,
			ImageRef: strings.TrimSpace(opts.ImageRef),
		}
		return &source, nil
	case hasGitHub:
		sourceType := model.AppSourceTypeGitHubPublic
		if opts.Private || strings.TrimSpace(opts.RepoToken) != "" {
			sourceType = model.AppSourceTypeGitHubPrivate
		}
		source := model.AppSource{
			Type:            sourceType,
			RepoURL:         strings.TrimSpace(opts.RepoURL),
			RepoBranch:      strings.TrimSpace(opts.Branch),
			RepoAuthToken:   strings.TrimSpace(opts.RepoToken),
			SourceDir:       strings.TrimSpace(opts.SourceDir),
			BuildStrategy:   strings.TrimSpace(opts.BuildStrategy),
			DockerfilePath:  strings.TrimSpace(opts.DockerfilePath),
			BuildContextDir: strings.TrimSpace(opts.BuildContextDir),
		}
		return &source, nil
	default:
		return nil, nil
	}
}

func appCreateServicePort(source model.AppSource, requested int, background bool) int {
	if background {
		return 0
	}
	if requested > 0 {
		return requested
	}
	switch strings.TrimSpace(source.Type) {
	case model.AppSourceTypeDockerImage:
		return 80
	default:
		switch strings.TrimSpace(source.BuildStrategy) {
		case model.AppBuildStrategyBuildpacks:
			return 8080
		case model.AppBuildStrategyNixpacks:
			return 3000
		case model.AppBuildStrategyStaticSite, model.AppBuildStrategyDockerfile, model.AppBuildStrategyAuto, "":
			return 80
		default:
			return 80
		}
	}
}

func applyCLIStartupCommand(spec *model.AppSpec, raw string) {
	trimmed := strings.TrimSpace(raw)
	if spec == nil || trimmed == "" {
		return
	}
	spec.Command = []string{"sh", "-lc", trimmed}
}

func (c *CLI) renderAppCreateResult(app model.App) error {
	pairs := []kvPair{
		{Key: "app", Value: app.Name},
		{Key: "phase", Value: strings.TrimSpace(app.Status.Phase)},
		{Key: "source", Value: sourceTypeForSync(app.Source)},
		{Key: "source_ref", Value: sourceRef(app.Source)},
		{Key: "next_step", Value: fmt.Sprintf("fugue app rebuild %s", app.Name)},
	}
	if c.showIDs() {
		pairs = append(pairs,
			kvPair{Key: "app_id", Value: app.ID},
			kvPair{Key: "project_id", Value: app.ProjectID},
			kvPair{Key: "tenant_id", Value: app.TenantID},
		)
	}
	return writeKeyValues(c.stdout, pairs...)
}
