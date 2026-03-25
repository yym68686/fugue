package cli

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"fugue/internal/model"
)

type DeployOptions struct {
	BaseURL         string
	Token           string
	WorkingDir      string
	TenantID        string
	TenantName      string
	AppID           string
	AppName         string
	ProjectID       string
	ProjectName     string
	Description     string
	BuildStrategy   string
	SourceDir       string
	DockerfilePath  string
	BuildContextDir string
	RuntimeID       string
	Replicas        int
	ServicePort     int
	Wait            bool
	PollInterval    time.Duration
}

func Run(args []string) error {
	if len(args) == 0 {
		printTopLevelUsage(os.Stderr)
		return fmt.Errorf("command is required")
	}
	switch args[0] {
	case "-h", "--help", "help":
		printTopLevelUsage(os.Stdout)
		return nil
	case "deploy":
		return runDeploy(args[1:])
	default:
		printTopLevelUsage(os.Stderr)
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func runDeploy(args []string) error {
	workingDir, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("get working directory: %w", err)
	}

	opts := DeployOptions{
		BaseURL:       firstNonEmpty(os.Getenv("FUGUE_BASE_URL")),
		Token:         firstNonEmpty(os.Getenv("FUGUE_TOKEN"), os.Getenv("FUGUE_API_KEY"), os.Getenv("FUGUE_BOOTSTRAP_KEY")),
		WorkingDir:    workingDir,
		TenantID:      firstNonEmpty(os.Getenv("FUGUE_TENANT_ID")),
		TenantName:    firstNonEmpty(os.Getenv("FUGUE_TENANT"), os.Getenv("FUGUE_TENANT_NAME")),
		ProjectName:   "default",
		BuildStrategy: model.AppBuildStrategyAuto,
		Wait:          true,
		PollInterval:  2 * time.Second,
	}

	fs := flag.NewFlagSet("deploy", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() {
		printDeployUsage(fs.Output())
	}
	fs.StringVar(&opts.BaseURL, "base-url", opts.BaseURL, "Fugue API base URL")
	fs.StringVar(&opts.Token, "token", opts.Token, "Fugue API token")
	fs.StringVar(&opts.WorkingDir, "dir", opts.WorkingDir, "project directory to deploy")
	fs.StringVar(&opts.TenantID, "tenant-id", opts.TenantID, "target tenant ID")
	fs.StringVar(&opts.TenantName, "tenant", opts.TenantName, "target tenant name or slug")
	fs.StringVar(&opts.AppID, "app-id", "", "existing app ID to redeploy")
	fs.StringVar(&opts.AppName, "name", "", "app name (defaults to current directory name)")
	fs.StringVar(&opts.ProjectID, "project-id", "", "target project ID")
	fs.StringVar(&opts.ProjectName, "project", opts.ProjectName, "target project name")
	fs.StringVar(&opts.Description, "description", "", "app description when creating a new app")
	fs.StringVar(&opts.BuildStrategy, "build-strategy", opts.BuildStrategy, "build strategy: auto, static-site, dockerfile, buildpacks, nixpacks")
	fs.StringVar(&opts.SourceDir, "source-dir", "", "source directory relative to project root")
	fs.StringVar(&opts.DockerfilePath, "dockerfile-path", "", "Dockerfile path relative to project root")
	fs.StringVar(&opts.BuildContextDir, "build-context-dir", "", "build context directory relative to project root")
	fs.StringVar(&opts.RuntimeID, "runtime-id", "", "target runtime ID")
	fs.IntVar(&opts.Replicas, "replicas", 0, "desired replica count")
	fs.IntVar(&opts.ServicePort, "service-port", 0, "service port override")
	fs.BoolVar(&opts.Wait, "wait", opts.Wait, "wait for the build/deploy operation to complete")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	if opts.AppName == "" {
		opts.AppName = model.Slugify(filepath.Base(strings.TrimSpace(opts.WorkingDir)))
	}
	if opts.AppName == "" {
		opts.AppName = "app"
	}

	client, err := NewClient(opts.BaseURL, opts.Token)
	if err != nil {
		return err
	}

	resolvedTenantID := ""
	resolvedProjectID := ""
	var createProjectRequest *importUploadProjectRequest
	if strings.TrimSpace(opts.AppID) == "" {
		resolvedTenantID, err = resolveTenantSelection(client, opts.TenantID, opts.TenantName)
		if err != nil {
			return err
		}

		resolvedProjectID, createProjectRequest, err = resolveProjectSelection(client, resolvedTenantID, opts.ProjectID, opts.ProjectName)
		if err != nil {
			return err
		}
	}

	resolvedAppID, err := resolveAppSelection(client, opts.AppID, opts.AppName, resolvedProjectID, resolvedTenantID)
	if err != nil {
		return err
	}

	archiveBytes, archiveName, err := createSourceArchive(opts.WorkingDir, opts.AppName)
	if err != nil {
		return err
	}

	request := importUploadRequest{
		AppID:           resolvedAppID,
		TenantID:        resolvedTenantID,
		SourceDir:       strings.TrimSpace(opts.SourceDir),
		Name:            opts.AppName,
		Description:     strings.TrimSpace(opts.Description),
		BuildStrategy:   strings.TrimSpace(opts.BuildStrategy),
		RuntimeID:       strings.TrimSpace(opts.RuntimeID),
		Replicas:        opts.Replicas,
		ServicePort:     opts.ServicePort,
		DockerfilePath:  strings.TrimSpace(opts.DockerfilePath),
		BuildContextDir: strings.TrimSpace(opts.BuildContextDir),
	}
	if resolvedAppID == "" {
		if resolvedProjectID != "" {
			request.ProjectID = resolvedProjectID
		} else if createProjectRequest != nil {
			request.Project = createProjectRequest
		}
	}

	fmt.Fprintf(os.Stderr, "Uploading %s (%d bytes)\n", archiveName, len(archiveBytes))
	app, op, err := client.ImportUpload(request, archiveName, archiveBytes)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "Queued operation %s for app %s\n", op.ID, app.ID)

	if !opts.Wait {
		fmt.Printf("app_id=%s\noperation_id=%s\n", app.ID, op.ID)
		return nil
	}

	lastStatus := ""
	for {
		currentOp, err := client.GetOperation(op.ID)
		if err != nil {
			return err
		}
		status := strings.TrimSpace(currentOp.Status)
		if status != lastStatus {
			fmt.Fprintf(os.Stderr, "operation_status=%s\n", status)
			lastStatus = status
		}
		switch status {
		case model.OperationStatusCompleted:
			finalApp, err := client.GetApp(app.ID)
			if err != nil {
				return err
			}
			if finalApp.Route != nil && strings.TrimSpace(finalApp.Route.PublicURL) != "" {
				fmt.Printf("app_id=%s\nurl=%s\n", finalApp.ID, finalApp.Route.PublicURL)
			} else {
				fmt.Printf("app_id=%s\n", finalApp.ID)
			}
			return nil
		case model.OperationStatusFailed:
			logs, logsErr := client.GetBuildLogs(app.ID, op.ID)
			if logsErr == nil && strings.TrimSpace(logs) != "" {
				return fmt.Errorf("operation %s failed: %s\n\n%s", op.ID, strings.TrimSpace(currentOp.ErrorMessage), strings.TrimSpace(logs))
			}
			if strings.TrimSpace(currentOp.ErrorMessage) != "" {
				return fmt.Errorf("operation %s failed: %s", op.ID, strings.TrimSpace(currentOp.ErrorMessage))
			}
			return fmt.Errorf("operation %s failed", op.ID)
		}
		time.Sleep(opts.PollInterval)
	}
}

func resolveTenantSelection(client *Client, tenantID, tenantName string) (string, error) {
	tenantID = strings.TrimSpace(tenantID)
	tenantName = strings.TrimSpace(tenantName)
	if tenantID != "" {
		return tenantID, nil
	}

	tenants, err := client.ListTenants()
	if err != nil {
		return "", err
	}
	if len(tenants) == 0 {
		return "", fmt.Errorf("no visible tenants; create a tenant first or pass --tenant-id")
	}
	if tenantName != "" {
		slug := model.Slugify(tenantName)
		matches := make([]model.Tenant, 0, 1)
		for _, tenant := range tenants {
			switch {
			case strings.EqualFold(tenant.ID, tenantName):
				matches = append(matches, tenant)
			case strings.EqualFold(tenant.Name, tenantName):
				matches = append(matches, tenant)
			case slug != "" && strings.EqualFold(tenant.Slug, slug):
				matches = append(matches, tenant)
			}
		}
		switch len(matches) {
		case 0:
			return "", fmt.Errorf("tenant %q not found", tenantName)
		case 1:
			return matches[0].ID, nil
		default:
			return "", fmt.Errorf("multiple tenants match %q; pass --tenant-id", tenantName)
		}
	}
	if len(tenants) == 1 {
		return tenants[0].ID, nil
	}
	return "", fmt.Errorf("multiple tenants are visible; pass --tenant or --tenant-id")
}

func resolveProjectSelection(client *Client, tenantID, projectID, projectName string) (string, *importUploadProjectRequest, error) {
	projectID = strings.TrimSpace(projectID)
	projectName = strings.TrimSpace(projectName)
	if projectID != "" {
		return projectID, nil, nil
	}
	if projectName == "" || strings.EqualFold(projectName, "default") {
		return "", nil, nil
	}
	if strings.TrimSpace(tenantID) == "" {
		return "", nil, fmt.Errorf("tenant id is required to resolve project %q", projectName)
	}
	projects, err := client.ListProjects(tenantID)
	if err != nil {
		return "", nil, err
	}
	for _, project := range projects {
		if strings.EqualFold(project.Name, projectName) || strings.EqualFold(project.Slug, model.Slugify(projectName)) {
			return project.ID, nil, nil
		}
	}
	return "", &importUploadProjectRequest{Name: projectName}, nil
}

func resolveAppSelection(client *Client, appID, appName, projectID, tenantID string) (string, error) {
	appID = strings.TrimSpace(appID)
	if appID != "" {
		return appID, nil
	}
	apps, err := client.ListApps()
	if err != nil {
		return "", err
	}
	name := model.Slugify(appName)
	matches := make([]model.App, 0, 1)
	for _, app := range apps {
		if tenantID != "" && app.TenantID != tenantID {
			continue
		}
		if !strings.EqualFold(app.Name, name) {
			continue
		}
		if projectID != "" && app.ProjectID != projectID {
			continue
		}
		matches = append(matches, app)
	}
	switch len(matches) {
	case 0:
		return "", nil
	case 1:
		return matches[0].ID, nil
	default:
		return "", fmt.Errorf("multiple apps named %q are visible; pass --app-id or --project-id", name)
	}
}

func createSourceArchive(rootDir, appName string) ([]byte, string, error) {
	rootDir = strings.TrimSpace(rootDir)
	if rootDir == "" {
		return nil, "", fmt.Errorf("project directory is required")
	}
	buffer := bytes.NewBuffer(nil)
	gzipWriter := gzip.NewWriter(buffer)
	tarWriter := tar.NewWriter(gzipWriter)

	err := filepath.WalkDir(rootDir, func(fullPath string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relPath, err := filepath.Rel(rootDir, fullPath)
		if err != nil {
			return err
		}
		relPath = filepath.ToSlash(relPath)
		if relPath == "." {
			return nil
		}
		if skip, skipDir := shouldSkipArchivePath(relPath, entry); skip {
			if skipDir {
				return filepath.SkipDir
			}
			return nil
		}

		info, err := entry.Info()
		if err != nil {
			return err
		}
		linkTarget := ""
		if info.Mode()&os.ModeSymlink != 0 {
			linkTarget, err = os.Readlink(fullPath)
			if err != nil {
				return err
			}
		}

		header, err := tar.FileInfoHeader(info, linkTarget)
		if err != nil {
			return err
		}
		header.Name = relPath
		if info.IsDir() && !strings.HasSuffix(header.Name, "/") {
			header.Name += "/"
		}
		if err := tarWriter.WriteHeader(header); err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}

		file, err := os.Open(fullPath)
		if err != nil {
			return err
		}
		if _, err := io.Copy(tarWriter, file); err != nil {
			_ = file.Close()
			return err
		}
		if err := file.Close(); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, "", fmt.Errorf("archive source directory: %w", err)
	}
	if err := tarWriter.Close(); err != nil {
		return nil, "", fmt.Errorf("close tar archive: %w", err)
	}
	if err := gzipWriter.Close(); err != nil {
		return nil, "", fmt.Errorf("close gzip archive: %w", err)
	}
	return buffer.Bytes(), model.Slugify(appName) + ".tgz", nil
}

func shouldSkipArchivePath(relPath string, entry fs.DirEntry) (bool, bool) {
	segments := strings.Split(relPath, "/")
	for _, segment := range segments {
		switch segment {
		case ".git", "node_modules", ".next", ".turbo", ".vercel", ".idea", ".vscode", "__pycache__":
			return true, entry.IsDir()
		}
	}
	switch filepath.Base(relPath) {
	case ".DS_Store", "Thumbs.db":
		return true, false
	default:
		return false, false
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func printTopLevelUsage(w io.Writer) {
	_, _ = fmt.Fprintln(w, "Usage: fugue deploy [flags]")
}

func printDeployUsage(w io.Writer) {
	_, _ = fmt.Fprintln(w, "Usage: fugue deploy [flags]")
	_, _ = fmt.Fprintln(w)
	_, _ = fmt.Fprintln(w, "Required environment or flags:")
	_, _ = fmt.Fprintln(w, "  FUGUE_BASE_URL / --base-url")
	_, _ = fmt.Fprintln(w, "  FUGUE_TOKEN / --token")
	_, _ = fmt.Fprintln(w)
	_, _ = fmt.Fprintln(w, "Common examples:")
	_, _ = fmt.Fprintln(w, "  fugue deploy --name cerebr")
	_, _ = fmt.Fprintln(w, "  fugue deploy --tenant my-tenant --project default --name cerebr")
	_, _ = fmt.Fprintln(w, "  fugue deploy --app-id <app-id>")
	_, _ = fmt.Fprintln(w)
	_, _ = fmt.Fprintln(w, "Flags:")
	fs := []string{
		"  --base-url string           Fugue API base URL",
		"  --token string              Fugue API token",
		"  --dir string                project directory to deploy",
		"  --tenant-id string          target tenant ID",
		"  --tenant string             target tenant name or slug",
		"  --project-id string         target project ID",
		"  --project string            target project name",
		"  --app-id string             existing app ID to redeploy",
		"  --name string               app name",
		"  --build-strategy string     auto, static-site, dockerfile, buildpacks, nixpacks",
		"  --source-dir string         source directory relative to project root",
		"  --dockerfile-path string    Dockerfile path relative to project root",
		"  --build-context-dir string  build context directory relative to project root",
		"  --runtime-id string         target runtime ID",
		"  --replicas int              desired replica count",
		"  --service-port int          service port override",
		"  --wait                      wait for operation completion (default true)",
	}
	for _, line := range fs {
		_, _ = fmt.Fprintln(w, line)
	}
}
