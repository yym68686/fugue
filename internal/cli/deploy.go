package cli

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"fugue/internal/model"
)

type projectSelection struct {
	ID     string
	Create *importProjectRequest
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
		return "", fmt.Errorf("no visible tenants; ask an admin for access or create one with \"fugue admin tenant create <name>\"")
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
			return "", fmt.Errorf("multiple tenants match %q; rerun with an exact --tenant value", tenantName)
		}
	}
	if len(tenants) == 1 {
		return tenants[0].ID, nil
	}
	return "", fmt.Errorf("multiple tenants are visible; rerun with --tenant <name>")
}

func resolveProjectCreationSelection(client *Client, tenantID, projectID, projectName string) (projectSelection, error) {
	projectID = strings.TrimSpace(projectID)
	projectName = strings.TrimSpace(projectName)
	if projectID != "" {
		return projectSelection{ID: projectID}, nil
	}
	if projectName == "" || strings.EqualFold(projectName, "default") {
		return projectSelection{}, nil
	}
	if strings.TrimSpace(tenantID) == "" {
		return projectSelection{}, fmt.Errorf("project %q needs a tenant context; rerun with --tenant <name>", projectName)
	}
	projects, err := client.ListProjects(tenantID)
	if err != nil {
		return projectSelection{}, err
	}
	for _, project := range projects {
		switch {
		case strings.EqualFold(project.ID, projectName):
			return projectSelection{ID: project.ID}, nil
		case strings.EqualFold(project.Name, projectName):
			return projectSelection{ID: project.ID}, nil
		case strings.EqualFold(project.Slug, model.Slugify(projectName)):
			return projectSelection{ID: project.ID}, nil
		}
	}
	return projectSelection{Create: &importProjectRequest{Name: projectName}}, nil
}

func resolveProjectSelection(client *Client, tenantID, projectID, projectName string) (string, *importProjectRequest, error) {
	selection, err := resolveProjectCreationSelection(client, tenantID, projectID, projectName)
	if err != nil {
		return "", nil, err
	}
	return selection.ID, selection.Create, nil
}

func resolveProjectReference(client *Client, tenantID, projectID, projectName string) (string, error) {
	projectID = strings.TrimSpace(projectID)
	if projectID != "" {
		return projectID, nil
	}
	projectName = strings.TrimSpace(projectName)
	if projectName == "" {
		return "", nil
	}
	if strings.TrimSpace(tenantID) == "" {
		return "", fmt.Errorf("project %q needs a tenant context; rerun with --tenant <name>", projectName)
	}
	projects, err := client.ListProjects(tenantID)
	if err != nil {
		return "", err
	}
	matches := make([]model.Project, 0, 1)
	for _, project := range projects {
		switch {
		case strings.EqualFold(project.ID, projectName):
			matches = append(matches, project)
		case strings.EqualFold(project.Name, projectName):
			matches = append(matches, project)
		case strings.EqualFold(project.Slug, model.Slugify(projectName)):
			matches = append(matches, project)
		}
	}
	switch len(matches) {
	case 0:
		return "", fmt.Errorf("project %q not found", projectName)
	case 1:
		return matches[0].ID, nil
	default:
		return "", fmt.Errorf("multiple projects match %q; rerun with a more specific --project value", projectName)
	}
}

func resolveAppSelection(client *Client, appID, appName, projectID, tenantID string) (string, error) {
	appID = strings.TrimSpace(appID)
	if appID != "" {
		return appID, nil
	}
	appName = strings.TrimSpace(appName)
	if appName == "" {
		return "", nil
	}
	apps, err := client.ListApps()
	if err != nil {
		return "", err
	}
	matches := matchVisibleApps(apps, appName, projectID, tenantID)
	switch len(matches) {
	case 0:
		return "", nil
	case 1:
		return matches[0].ID, nil
	default:
		return "", fmt.Errorf("multiple apps match %q; rerun with --project <name> or use an app id", appName)
	}
}

func resolveAppReference(client *Client, appRef, projectID, tenantID string) (model.App, error) {
	appRef = strings.TrimSpace(appRef)
	if appRef == "" {
		return model.App{}, fmt.Errorf("app is required")
	}
	apps, err := client.ListApps()
	if err != nil {
		return model.App{}, err
	}
	matches := matchVisibleApps(apps, appRef, projectID, tenantID)
	switch len(matches) {
	case 0:
		return model.App{}, fmt.Errorf("app %q not found", appRef)
	case 1:
		return matches[0], nil
	default:
		return model.App{}, fmt.Errorf("multiple apps match %q; pass --project or use an app id", appRef)
	}
}

func resolveRuntimeSelection(client *Client, runtimeID, runtimeName string) (string, error) {
	runtimeID = strings.TrimSpace(runtimeID)
	if runtimeID != "" {
		return runtimeID, nil
	}
	runtimeName = strings.TrimSpace(runtimeName)
	if runtimeName == "" {
		return "", nil
	}
	switch strings.ToLower(runtimeName) {
	case "shared", "managed-shared", "runtime_managed_shared":
		return "runtime_managed_shared", nil
	}
	runtimes, err := client.ListRuntimes()
	if err != nil {
		return "", err
	}
	matches := make([]model.Runtime, 0, 1)
	for _, runtimeObj := range runtimes {
		switch {
		case strings.EqualFold(runtimeObj.ID, runtimeName):
			matches = append(matches, runtimeObj)
		case strings.EqualFold(runtimeObj.Name, runtimeName):
			matches = append(matches, runtimeObj)
		case strings.EqualFold(model.Slugify(runtimeObj.Name), model.Slugify(runtimeName)):
			matches = append(matches, runtimeObj)
		}
	}
	switch len(matches) {
	case 0:
		return "", fmt.Errorf("runtime %q not found", runtimeName)
	case 1:
		return matches[0].ID, nil
	default:
		return "", fmt.Errorf("multiple runtimes match %q; rerun with the exact runtime name or use --runtime-id", runtimeName)
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

func defaultDeployAppName(workingDir, repoURL string) string {
	if repoName := repoURLAppName(repoURL); repoName != "" {
		return repoName
	}
	return model.Slugify(filepath.Base(strings.TrimSpace(workingDir)))
}

func repoURLAppName(repoURL string) string {
	repoURL = strings.TrimSpace(strings.TrimSuffix(repoURL, ".git"))
	if repoURL == "" {
		return ""
	}
	parts := strings.Split(strings.Trim(repoURL, "/"), "/")
	if len(parts) == 0 {
		return ""
	}
	return model.Slugify(parts[len(parts)-1])
}

func normalizeGitHubRepoArg(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if strings.HasPrefix(raw, "git@github.com:") {
		repo := strings.TrimSpace(strings.TrimPrefix(raw, "git@github.com:"))
		repo = strings.TrimSuffix(repo, ".git")
		return "https://github.com/" + strings.Trim(repo, "/")
	}
	if strings.Contains(raw, "://") {
		return raw
	}
	return "https://github.com/" + strings.Trim(strings.TrimSuffix(raw, ".git"), "/")
}

func imageRefAppName(imageRef string) string {
	imageRef = strings.TrimSpace(imageRef)
	if imageRef == "" {
		return ""
	}
	if idx := strings.Index(imageRef, "@"); idx >= 0 {
		imageRef = imageRef[:idx]
	}
	lastSegment := imageRef
	if idx := strings.LastIndex(lastSegment, "/"); idx >= 0 {
		lastSegment = lastSegment[idx+1:]
	}
	if idx := strings.Index(lastSegment, ":"); idx >= 0 {
		lastSegment = lastSegment[:idx]
	}
	return model.Slugify(lastSegment)
}

func matchVisibleApps(apps []model.App, ref, projectID, tenantID string) []model.App {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return nil
	}
	slug := model.Slugify(ref)
	matches := make([]model.App, 0, 1)
	for _, app := range apps {
		if tenantID != "" && app.TenantID != tenantID {
			continue
		}
		if projectID != "" && app.ProjectID != projectID {
			continue
		}
		switch {
		case strings.EqualFold(app.ID, ref):
			matches = append(matches, app)
		case strings.EqualFold(app.Name, ref):
			matches = append(matches, app)
		case slug != "" && strings.EqualFold(app.Name, slug):
			matches = append(matches, app)
		}
	}
	return matches
}
