package api

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"os/exec"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

const (
	defaultFilesystemReadMaxBytes = 256 * 1024

	filesystemMarkerNotFound      = "__FUGUE_NOT_FOUND__"
	filesystemMarkerNotDirectory  = "__FUGUE_NOT_DIRECTORY__"
	filesystemMarkerIsDirectory   = "__FUGUE_IS_DIRECTORY__"
	filesystemMarkerParentMissing = "__FUGUE_PARENT_MISSING__"
)

type workspacePodLister interface {
	listPodsBySelector(ctx context.Context, namespace, selector string) ([]kubePodInfo, error)
}

type workspacePodExecRunner interface {
	Run(ctx context.Context, namespace, podName, containerName string, stdin []byte, command ...string) ([]byte, error)
}

type kubectlWorkspaceExecRunner struct{}

type filesystemAPIError struct {
	StatusCode int
	Message    string
	Err        error
}

type appFilesystemTarget struct {
	namespace     string
	component     string
	podName       string
	containerName string
	workspaceRoot string
}

type appFilesystemEntry struct {
	Name        string    `json:"name"`
	Path        string    `json:"path"`
	Kind        string    `json:"kind"`
	Size        int64     `json:"size"`
	Mode        int32     `json:"mode,omitempty"`
	ModifiedAt  time.Time `json:"modified_at"`
	HasChildren bool      `json:"has_children"`
}

func (e *filesystemAPIError) Error() string {
	if e == nil {
		return ""
	}
	if e.Err != nil {
		return e.Message + ": " + e.Err.Error()
	}
	return e.Message
}

func (r kubectlWorkspaceExecRunner) Run(ctx context.Context, namespace, podName, containerName string, stdin []byte, command ...string) ([]byte, error) {
	args := []string{"-n", namespace, "exec", strings.TrimSpace(podName)}
	if name := strings.TrimSpace(containerName); name != "" {
		args = append(args, "-c", name)
	}
	args = append(args, "--")
	args = append(args, command...)

	cmd := exec.CommandContext(ctx, "kubectl", args...)
	if len(stdin) > 0 {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		trimmed := strings.TrimSpace(string(output))
		if trimmed == "" {
			trimmed = err.Error()
		}
		return nil, fmt.Errorf("kubectl exec pod %s container %s failed: %s", podName, containerName, trimmed)
	}
	return output, nil
}

func (s *Server) handleGetAppFilesystemTree(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}

	component, err := parseFilesystemComponent(r.URL.Query().Get("component"))
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	depth, err := parseFilesystemDepth(r.URL.Query().Get("depth"))
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	target, err := s.resolveAppFilesystemTarget(r.Context(), app, component, r.URL.Query().Get("pod"))
	if err != nil {
		writeFilesystemError(w, err)
		return
	}
	requestPath, err := normalizeFilesystemPath(target.workspaceRoot, r.URL.Query().Get("path"), true)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	out, err := s.runAppFilesystemCommand(r.Context(), target, nil, filesystemTreeCommand(requestPath)...)
	if err != nil {
		writeFilesystemError(w, err)
		return
	}

	entries, err := parseFilesystemTreeEntries(out)
	if err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.appendAudit(principal, "app.filesystem.tree.read", "app", app.ID, app.TenantID, map[string]string{
		"component": component,
		"path":      requestPath,
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"component":      component,
		"pod":            target.podName,
		"path":           requestPath,
		"depth":          depth,
		"workspace_root": target.workspaceRoot,
		"entries":        entries,
	})
}

func (s *Server) handleGetAppFilesystemFile(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}

	component, err := parseFilesystemComponent(r.URL.Query().Get("component"))
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	maxBytes, err := parseFilesystemReadMaxBytes(r.URL.Query().Get("max_bytes"))
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	target, err := s.resolveAppFilesystemTarget(r.Context(), app, component, r.URL.Query().Get("pod"))
	if err != nil {
		writeFilesystemError(w, err)
		return
	}
	requestPath, err := normalizeFilesystemPath(target.workspaceRoot, r.URL.Query().Get("path"), false)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	out, err := s.runAppFilesystemCommand(r.Context(), target, nil, filesystemReadFileCommand(requestPath, maxBytes)...)
	if err != nil {
		writeFilesystemError(w, err)
		return
	}

	size, mode, modifiedAt, contentBytes, err := parseFilesystemReadFileOutput(out)
	if err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	encoding, content := encodeFilesystemContent(contentBytes)

	s.appendAudit(principal, "app.filesystem.file.read", "app", app.ID, app.TenantID, map[string]string{
		"component": component,
		"path":      requestPath,
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"component":      component,
		"pod":            target.podName,
		"path":           requestPath,
		"workspace_root": target.workspaceRoot,
		"content":        content,
		"encoding":       encoding,
		"size":           size,
		"mode":           mode,
		"modified_at":    modifiedAt,
		"truncated":      size > int64(maxBytes),
	})
}

func (s *Server) handlePutAppFilesystemFile(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.write") && !principal.HasScope("app.deploy") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.write or app.deploy scope")
		return
	}
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}

	component, err := parseFilesystemComponent(r.URL.Query().Get("component"))
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	var req struct {
		Path         string `json:"path"`
		Content      string `json:"content"`
		Encoding     string `json:"encoding"`
		Mode         int32  `json:"mode"`
		MkdirParents bool   `json:"mkdir_parents"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	target, err := s.resolveAppFilesystemTarget(r.Context(), app, component, r.URL.Query().Get("pod"))
	if err != nil {
		writeFilesystemError(w, err)
		return
	}
	requestPath, err := normalizeFilesystemPath(target.workspaceRoot, req.Path, false)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	contentBytes, err := decodeFilesystemContent(req.Content, req.Encoding)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	out, err := s.runAppFilesystemCommand(r.Context(), target, contentBytes, filesystemWriteFileCommand(requestPath, req.MkdirParents, req.Mode)...)
	if err != nil {
		writeFilesystemError(w, err)
		return
	}
	size, mode, modifiedAt, err := parseFilesystemMetadataLine(out)
	if err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.appendAudit(principal, "app.filesystem.file.write", "app", app.ID, app.TenantID, map[string]string{
		"component": component,
		"path":      requestPath,
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"component":      component,
		"pod":            target.podName,
		"path":           requestPath,
		"workspace_root": target.workspaceRoot,
		"size":           size,
		"mode":           mode,
		"modified_at":    modifiedAt,
	})
}

func (s *Server) handleCreateAppFilesystemDirectory(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.write") && !principal.HasScope("app.deploy") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.write or app.deploy scope")
		return
	}
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}

	component, err := parseFilesystemComponent(r.URL.Query().Get("component"))
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	var req struct {
		Path    string `json:"path"`
		Mode    int32  `json:"mode"`
		Parents bool   `json:"parents"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	target, err := s.resolveAppFilesystemTarget(r.Context(), app, component, r.URL.Query().Get("pod"))
	if err != nil {
		writeFilesystemError(w, err)
		return
	}
	requestPath, err := normalizeFilesystemPath(target.workspaceRoot, req.Path, false)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	out, err := s.runAppFilesystemCommand(r.Context(), target, nil, filesystemCreateDirectoryCommand(requestPath, req.Parents, req.Mode)...)
	if err != nil {
		writeFilesystemError(w, err)
		return
	}
	size, mode, modifiedAt, err := parseFilesystemMetadataLine(out)
	if err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.appendAudit(principal, "app.filesystem.directory.create", "app", app.ID, app.TenantID, map[string]string{
		"component": component,
		"path":      requestPath,
	})
	httpx.WriteJSON(w, http.StatusCreated, map[string]any{
		"component":      component,
		"pod":            target.podName,
		"path":           requestPath,
		"workspace_root": target.workspaceRoot,
		"kind":           "dir",
		"size":           size,
		"mode":           mode,
		"modified_at":    modifiedAt,
	})
}

func (s *Server) handleDeleteAppFilesystemPath(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.write") && !principal.HasScope("app.deploy") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.write or app.deploy scope")
		return
	}
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}

	component, err := parseFilesystemComponent(r.URL.Query().Get("component"))
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	recursive, err := parseBoolQuery(r.URL.Query().Get("recursive"))
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	target, err := s.resolveAppFilesystemTarget(r.Context(), app, component, r.URL.Query().Get("pod"))
	if err != nil {
		writeFilesystemError(w, err)
		return
	}
	requestPath, err := normalizeFilesystemPath(target.workspaceRoot, r.URL.Query().Get("path"), false)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	if _, err := s.runAppFilesystemCommand(r.Context(), target, nil, filesystemDeletePathCommand(requestPath, recursive)...); err != nil {
		writeFilesystemError(w, err)
		return
	}

	s.appendAudit(principal, "app.filesystem.path.delete", "app", app.ID, app.TenantID, map[string]string{
		"component": component,
		"path":      requestPath,
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"component":      component,
		"pod":            target.podName,
		"path":           requestPath,
		"workspace_root": target.workspaceRoot,
		"deleted":        true,
	})
}

func (s *Server) resolveAppFilesystemTarget(ctx context.Context, app model.App, component, requestedPod string) (appFilesystemTarget, error) {
	if component != "app" {
		return appFilesystemTarget{}, &filesystemAPIError{StatusCode: http.StatusBadRequest, Message: "component must be app"}
	}
	if app.Spec.Workspace == nil {
		return appFilesystemTarget{}, &filesystemAPIError{StatusCode: http.StatusBadRequest, Message: "app does not have a persistent workspace"}
	}

	workspaceRoot, err := model.NormalizeAppWorkspaceMountPath(app.Spec.Workspace.MountPath)
	if err != nil {
		return appFilesystemTarget{}, &filesystemAPIError{StatusCode: http.StatusBadRequest, Message: "app workspace mount_path is invalid", Err: err}
	}

	runtimeObj, err := s.store.GetRuntime(app.Spec.RuntimeID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return appFilesystemTarget{}, &filesystemAPIError{StatusCode: http.StatusBadRequest, Message: "app runtime is not available"}
		}
		return appFilesystemTarget{}, err
	}
	if runtimeObj.Type != model.RuntimeTypeManagedOwned {
		return appFilesystemTarget{}, &filesystemAPIError{StatusCode: http.StatusBadRequest, Message: "persistent workspace requires a managed-owned runtime"}
	}

	listerFactory := s.newWorkspacePodLister
	if listerFactory == nil {
		listerFactory = func(namespace string) (workspacePodLister, error) {
			return newKubeLogsClient(namespace)
		}
	}

	namespace := runtime.NamespaceForTenant(app.TenantID)
	lister, err := listerFactory(namespace)
	if err != nil {
		return appFilesystemTarget{}, &filesystemAPIError{StatusCode: http.StatusServiceUnavailable, Message: "workspace access is not available", Err: err}
	}

	selector, _, err := runtimeLogTarget(app, "app")
	if err != nil {
		return appFilesystemTarget{}, &filesystemAPIError{StatusCode: http.StatusBadRequest, Message: err.Error()}
	}

	pods, err := lister.listPodsBySelector(ctx, namespace, selector)
	if err != nil {
		return appFilesystemTarget{}, &filesystemAPIError{StatusCode: http.StatusInternalServerError, Message: "list app pods", Err: err}
	}
	sortPodsByCreation(pods)
	pods = filterPodsByName(pods, requestedPod)
	if len(pods) == 0 {
		if strings.TrimSpace(requestedPod) != "" {
			return appFilesystemTarget{}, &filesystemAPIError{StatusCode: http.StatusNotFound, Message: "no matching pods found"}
		}
		if app.Spec.Replicas <= 0 || app.Status.CurrentReplicas <= 0 {
			return appFilesystemTarget{}, &filesystemAPIError{StatusCode: http.StatusConflict, Message: "app has no running pod for workspace access"}
		}
		return appFilesystemTarget{}, &filesystemAPIError{StatusCode: http.StatusNotFound, Message: "no running app pods found"}
	}

	pod := chooseFilesystemPod(pods)
	if pod.Metadata.Name == "" {
		return appFilesystemTarget{}, &filesystemAPIError{StatusCode: http.StatusNotFound, Message: "no running app pods found"}
	}
	return appFilesystemTarget{
		namespace:     namespace,
		component:     component,
		podName:       pod.Metadata.Name,
		containerName: runtime.AppWorkspaceContainerName,
		workspaceRoot: workspaceRoot,
	}, nil
}

func chooseFilesystemPod(pods []kubePodInfo) kubePodInfo {
	for index := len(pods) - 1; index >= 0; index-- {
		if strings.EqualFold(strings.TrimSpace(pods[index].Status.Phase), "Running") {
			return pods[index]
		}
	}
	if len(pods) == 0 {
		return kubePodInfo{}
	}
	return pods[len(pods)-1]
}

func (s *Server) runAppFilesystemCommand(ctx context.Context, target appFilesystemTarget, stdin []byte, command ...string) ([]byte, error) {
	runner := s.workspaceExecRunner
	if runner == nil {
		runner = kubectlWorkspaceExecRunner{}
	}

	commandCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	out, err := runner.Run(commandCtx, target.namespace, target.podName, target.containerName, stdin, command...)
	if err != nil {
		return nil, mapFilesystemExecError(err)
	}
	return out, nil
}

func writeFilesystemError(w http.ResponseWriter, err error) {
	var apiErr *filesystemAPIError
	switch {
	case errors.As(err, &apiErr):
		httpx.WriteError(w, apiErr.StatusCode, apiErr.Message)
	case errors.Is(err, exec.ErrNotFound):
		httpx.WriteError(w, http.StatusServiceUnavailable, "kubectl is not available in the api runtime")
	case errors.Is(err, store.ErrNotFound):
		httpx.WriteError(w, http.StatusNotFound, "resource not found")
	default:
		httpx.WriteError(w, http.StatusInternalServerError, err.Error())
	}
}

func mapFilesystemExecError(err error) error {
	if err == nil {
		return nil
	}
	message := err.Error()
	switch {
	case strings.Contains(message, filesystemMarkerNotFound):
		return &filesystemAPIError{StatusCode: http.StatusNotFound, Message: "path not found", Err: err}
	case strings.Contains(message, filesystemMarkerNotDirectory):
		return &filesystemAPIError{StatusCode: http.StatusBadRequest, Message: "path must reference a directory", Err: err}
	case strings.Contains(message, filesystemMarkerIsDirectory):
		return &filesystemAPIError{StatusCode: http.StatusBadRequest, Message: "path must reference a file", Err: err}
	case strings.Contains(message, filesystemMarkerParentMissing):
		return &filesystemAPIError{StatusCode: http.StatusBadRequest, Message: "parent directory does not exist", Err: err}
	default:
		return err
	}
}

func parseFilesystemComponent(raw string) (string, error) {
	component := strings.TrimSpace(strings.ToLower(raw))
	if component == "" {
		component = "app"
	}
	if component != "app" {
		return "", fmt.Errorf("component must be app")
	}
	return component, nil
}

func parseFilesystemDepth(raw string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 1, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("depth must be an integer")
	}
	if value != 1 {
		return 0, fmt.Errorf("only depth=1 is currently supported")
	}
	return value, nil
}

func parseFilesystemReadMaxBytes(raw string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return defaultFilesystemReadMaxBytes, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("max_bytes must be an integer")
	}
	if value < 1 || value > 10*1024*1024 {
		return 0, fmt.Errorf("max_bytes must be between 1 and 10485760")
	}
	return value, nil
}

func normalizeFilesystemPath(workspaceRoot, rawPath string, allowRoot bool) (string, error) {
	if strings.TrimSpace(rawPath) == "" {
		rawPath = workspaceRoot
	}
	cleaned, err := model.NormalizeAbsolutePath(rawPath)
	if err != nil {
		return "", fmt.Errorf("path must be absolute")
	}
	if !model.PathWithinBase(workspaceRoot, cleaned) {
		return "", fmt.Errorf("path must be inside the app workspace %s", workspaceRoot)
	}
	if !allowRoot && cleaned == path.Clean(workspaceRoot) {
		return "", fmt.Errorf("path must not be the workspace root")
	}
	if model.PathWithinBase(model.AppWorkspaceInternalPath(workspaceRoot), cleaned) {
		return "", fmt.Errorf("path is reserved for fugue workspace metadata")
	}
	return cleaned, nil
}

func decodeFilesystemContent(content, encoding string) ([]byte, error) {
	switch strings.TrimSpace(strings.ToLower(encoding)) {
	case "", "utf-8", "utf8", "text":
		return []byte(content), nil
	case "base64":
		decoded, err := base64.StdEncoding.DecodeString(content)
		if err != nil {
			return nil, fmt.Errorf("decode base64 content: %w", err)
		}
		return decoded, nil
	default:
		return nil, fmt.Errorf("encoding must be utf-8 or base64")
	}
}

func encodeFilesystemContent(content []byte) (string, string) {
	if utf8.Valid(content) {
		return "utf-8", string(content)
	}
	return "base64", base64.StdEncoding.EncodeToString(content)
}

func parseFilesystemTreeEntries(out []byte) ([]appFilesystemEntry, error) {
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) == 1 && lines[0] == "" {
		return []appFilesystemEntry{}, nil
	}
	entries := make([]appFilesystemEntry, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.Split(line, "\t")
		if len(parts) != 7 {
			return nil, fmt.Errorf("unexpected filesystem tree entry %q", line)
		}
		nameBytes, err := base64.StdEncoding.DecodeString(parts[0])
		if err != nil {
			return nil, fmt.Errorf("decode filesystem entry name: %w", err)
		}
		pathBytes, err := base64.StdEncoding.DecodeString(parts[1])
		if err != nil {
			return nil, fmt.Errorf("decode filesystem entry path: %w", err)
		}
		size, err := strconv.ParseInt(parts[3], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse filesystem entry size: %w", err)
		}
		mode, err := parseFilesystemMode(parts[4])
		if err != nil {
			return nil, fmt.Errorf("parse filesystem entry mode: %w", err)
		}
		modifiedAt, err := parseFilesystemTimestamp(parts[5])
		if err != nil {
			return nil, fmt.Errorf("parse filesystem entry modified_at: %w", err)
		}
		hasChildren, err := strconv.ParseBool(parts[6])
		if err != nil {
			return nil, fmt.Errorf("parse filesystem entry has_children: %w", err)
		}
		entryPath := string(pathBytes)
		if path.Base(entryPath) == model.AppWorkspaceInternalDirName {
			continue
		}
		entries = append(entries, appFilesystemEntry{
			Name:        string(nameBytes),
			Path:        entryPath,
			Kind:        parts[2],
			Size:        size,
			Mode:        mode,
			ModifiedAt:  modifiedAt,
			HasChildren: hasChildren,
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Kind != entries[j].Kind {
			if entries[i].Kind == "dir" {
				return true
			}
			if entries[j].Kind == "dir" {
				return false
			}
		}
		return entries[i].Name < entries[j].Name
	})
	if len(entries) == 0 {
		return []appFilesystemEntry{}, nil
	}
	return entries, nil
}

func parseFilesystemReadFileOutput(out []byte) (int64, int32, time.Time, []byte, error) {
	index := bytes.IndexByte(out, '\n')
	if index < 0 {
		return 0, 0, time.Time{}, nil, fmt.Errorf("filesystem read output missing metadata")
	}
	size, mode, modifiedAt, err := parseFilesystemMetadataLine(out[:index])
	if err != nil {
		return 0, 0, time.Time{}, nil, err
	}
	content := append([]byte(nil), out[index+1:]...)
	return size, mode, modifiedAt, content, nil
}

func parseFilesystemMetadataLine(out []byte) (int64, int32, time.Time, error) {
	line := strings.TrimSpace(string(out))
	parts := strings.Split(line, "\t")
	if len(parts) != 3 {
		return 0, 0, time.Time{}, fmt.Errorf("unexpected filesystem metadata %q", line)
	}
	size, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, time.Time{}, fmt.Errorf("parse filesystem size: %w", err)
	}
	mode, err := parseFilesystemMode(parts[1])
	if err != nil {
		return 0, 0, time.Time{}, fmt.Errorf("parse filesystem mode: %w", err)
	}
	modifiedAt, err := parseFilesystemTimestamp(parts[2])
	if err != nil {
		return 0, 0, time.Time{}, fmt.Errorf("parse filesystem modified_at: %w", err)
	}
	return size, mode, modifiedAt, nil
}

func parseFilesystemMode(raw string) (int32, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	value, err := strconv.ParseInt(raw, 8, 32)
	if err != nil {
		return 0, err
	}
	return int32(value), nil
}

func parseFilesystemTimestamp(raw string) (time.Time, error) {
	value, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(value, 0).UTC(), nil
}

func filesystemTreeCommand(targetPath string) []string {
	return []string{
		"sh",
		"-lc",
		filesystemTreeScript(),
		"sh",
		targetPath,
	}
}

func filesystemReadFileCommand(targetPath string, maxBytes int) []string {
	return []string{
		"sh",
		"-lc",
		filesystemReadFileScript(),
		"sh",
		targetPath,
		strconv.Itoa(maxBytes),
	}
}

func filesystemWriteFileCommand(targetPath string, mkdirParents bool, mode int32) []string {
	modeValue := ""
	if mode > 0 {
		modeValue = strconv.FormatInt(int64(mode), 8)
	}
	return []string{
		"sh",
		"-lc",
		filesystemWriteFileScript(),
		"sh",
		targetPath,
		strconv.FormatBool(mkdirParents),
		modeValue,
	}
}

func filesystemCreateDirectoryCommand(targetPath string, parents bool, mode int32) []string {
	modeValue := ""
	if mode > 0 {
		modeValue = strconv.FormatInt(int64(mode), 8)
	}
	return []string{
		"sh",
		"-lc",
		filesystemCreateDirectoryScript(),
		"sh",
		targetPath,
		strconv.FormatBool(parents),
		modeValue,
	}
}

func filesystemDeletePathCommand(targetPath string, recursive bool) []string {
	return []string{
		"sh",
		"-lc",
		filesystemDeletePathScript(),
		"sh",
		targetPath,
		strconv.FormatBool(recursive),
	}
}

func filesystemTreeScript() string {
	return `target="$1"
if [ ! -e "$target" ]; then
  printf '` + filesystemMarkerNotFound + `'
  exit 44
fi
if [ ! -d "$target" ]; then
  printf '` + filesystemMarkerNotDirectory + `'
  exit 45
fi
find "$target" -mindepth 1 -maxdepth 1 -exec sh -c '
  for entry do
    base="${entry##*/}"
    if [ "$base" = "` + model.AppWorkspaceInternalDirName + `" ]; then
      continue
    fi
    kind=file
    if [ -L "$entry" ]; then
      kind=symlink
    elif [ -d "$entry" ]; then
      kind=dir
    fi
    size="$(stat -c %s "$entry" 2>/dev/null || printf 0)"
    mode="$(stat -c %a "$entry" 2>/dev/null || printf "")"
    modified="$(stat -c %Y "$entry" 2>/dev/null || printf 0)"
    has_children=false
    if [ "$kind" = dir ] && find "$entry" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null | grep -q .; then
      has_children=true
    fi
    printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
      "$(printf %s "$base" | base64 | tr -d "\n")" \
      "$(printf %s "$entry" | base64 | tr -d "\n")" \
      "$kind" "$size" "$mode" "$modified" "$has_children"
  done
' sh {} +`
}

func filesystemReadFileScript() string {
	return `target="$1"
limit="$2"
if [ ! -e "$target" ]; then
  printf '` + filesystemMarkerNotFound + `'
  exit 44
fi
if [ -d "$target" ]; then
  printf '` + filesystemMarkerIsDirectory + `'
  exit 46
fi
size="$(stat -c %s "$target" 2>/dev/null || wc -c < "$target")"
mode="$(stat -c %a "$target" 2>/dev/null || printf "")"
modified="$(stat -c %Y "$target" 2>/dev/null || printf 0)"
printf "%s\t%s\t%s\n" "$size" "$mode" "$modified"
head -c "$limit" "$target"`
}

func filesystemWriteFileScript() string {
	return `target="$1"
mkdir_parents="$2"
mode="$3"
if [ -d "$target" ]; then
  printf '` + filesystemMarkerIsDirectory + `'
  exit 46
fi
dir="$(dirname "$target")"
if [ "$mkdir_parents" = true ]; then
  mkdir -p "$dir"
elif [ ! -d "$dir" ]; then
  printf '` + filesystemMarkerParentMissing + `'
  exit 47
fi
cat > "$target"
if [ -n "$mode" ]; then
  chmod "$mode" "$target"
fi
size="$(stat -c %s "$target" 2>/dev/null || wc -c < "$target")"
mode_out="$(stat -c %a "$target" 2>/dev/null || printf "")"
modified="$(stat -c %Y "$target" 2>/dev/null || printf 0)"
printf "%s\t%s\t%s" "$size" "$mode_out" "$modified"`
}

func filesystemCreateDirectoryScript() string {
	return `target="$1"
parents="$2"
mode="$3"
if [ -e "$target" ] && [ ! -d "$target" ]; then
  printf '` + filesystemMarkerNotDirectory + `'
  exit 45
fi
if [ "$parents" = true ]; then
  mkdir -p "$target"
else
  mkdir "$target"
fi
if [ -n "$mode" ]; then
  chmod "$mode" "$target"
fi
size="$(stat -c %s "$target" 2>/dev/null || printf 0)"
mode_out="$(stat -c %a "$target" 2>/dev/null || printf "")"
modified="$(stat -c %Y "$target" 2>/dev/null || printf 0)"
printf "%s\t%s\t%s" "$size" "$mode_out" "$modified"`
}

func filesystemDeletePathScript() string {
	return `target="$1"
recursive="$2"
if [ ! -e "$target" ] && [ ! -L "$target" ]; then
  printf '` + filesystemMarkerNotFound + `'
  exit 44
fi
if [ -L "$target" ]; then
  rm -f -- "$target"
elif [ -d "$target" ]; then
  if [ "$recursive" = true ]; then
    rm -rf -- "$target"
  else
    rmdir "$target"
  fi
else
  rm -f -- "$target"
fi`
}
