package api

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"fugue/internal/httpx"
	"fugue/internal/model"
)

func (s *Server) handleGetAppEnv(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}

	spec := cloneAppSpec(app.Spec)
	envDetails := appEnvDetails{}

	if appDeployBaselineNeedsRecovery(spec, app.Source) {
		spec, _, err := s.recoverAppDeployBaseline(app)
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		envDetails = mergedAppEnvDetails(app, spec)
	} else {
		envDetails = mergedAppEnvDetails(app, spec)
	}
	s.appendAudit(principal, "app.env.read", "app", app.ID, app.TenantID, nil)
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"env":     defaultStringMap(envDetails.Env),
		"entries": envDetails.Entries,
	})
}

func (s *Server) handlePatchAppEnv(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.write") && !principal.HasScope("app.deploy") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.write or app.deploy scope")
		return
	}
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}

	var req struct {
		Set    map[string]string `json:"set"`
		Delete []string          `json:"delete"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	spec, source, err := s.recoverAppDeployBaseline(app)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	env, changed, err := applyEnvPatch(spec.Env, req.Set, req.Delete)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	spec.Env = env
	if !changed {
		envDetails := mergedAppEnvDetails(app, spec)
		httpx.WriteJSON(w, http.StatusOK, map[string]any{
			"env":             defaultStringMap(envDetails.Env),
			"entries":         envDetails.Entries,
			"already_current": true,
		})
		return
	}

	op, err := s.store.CreateOperation(model.Operation{
		TenantID:            app.TenantID,
		Type:                model.OperationTypeDeploy,
		RequestedByType:     principal.ActorType,
		RequestedByID:       principal.ActorID,
		AppID:               app.ID,
		DesiredSpec:         &spec,
		DesiredSource:       source,
		DesiredOriginSource: model.AppOriginSource(app),
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	s.appendAudit(principal, "app.env.patch", "operation", op.ID, app.TenantID, map[string]string{"app_id": app.ID})
	envDetails := mergedAppEnvDetails(app, spec)
	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{
		"env":       defaultStringMap(envDetails.Env),
		"entries":   envDetails.Entries,
		"operation": sanitizeOperationForAPI(op),
	})
}

func (s *Server) handleGetAppFiles(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}
	s.appendAudit(principal, "app.files.read", "app", app.ID, app.TenantID, nil)
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"files": defaultAppFiles(cloneAppFiles(app.Spec.Files)),
	})
}

func (s *Server) handleUpsertAppFiles(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.write") && !principal.HasScope("app.deploy") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.write or app.deploy scope")
		return
	}
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}

	var req struct {
		Files []model.AppFile `json:"files"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	files, err := normalizeUploadedFiles(req.Files)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	spec, source, err := s.recoverAppDeployBaseline(app)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	currentFiles := cloneAppFiles(spec.Files)
	spec.Files, _ = upsertAppFiles(spec.Files, files)
	if appFilesEqual(currentFiles, spec.Files) {
		httpx.WriteJSON(w, http.StatusOK, map[string]any{
			"files":           defaultAppFiles(cloneAppFiles(spec.Files)),
			"already_current": true,
		})
		return
	}

	op, err := s.store.CreateOperation(model.Operation{
		TenantID:            app.TenantID,
		Type:                model.OperationTypeDeploy,
		RequestedByType:     principal.ActorType,
		RequestedByID:       principal.ActorID,
		AppID:               app.ID,
		DesiredSpec:         &spec,
		DesiredSource:       source,
		DesiredOriginSource: model.AppOriginSource(app),
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	s.appendAudit(principal, "app.files.upsert", "operation", op.ID, app.TenantID, map[string]string{"app_id": app.ID})
	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{
		"files":     defaultAppFiles(cloneAppFiles(spec.Files)),
		"operation": sanitizeOperationForAPI(op),
	})
}

func (s *Server) handleDeleteAppFiles(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.write") && !principal.HasScope("app.deploy") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.write or app.deploy scope")
		return
	}
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}

	paths, err := normalizeDeletedFilePaths(r.URL.Query()["path"])
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	spec, source, err := s.recoverAppDeployBaseline(app)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	currentFiles := cloneAppFiles(spec.Files)
	spec.Files, _ = deleteAppFiles(spec.Files, paths)
	if appFilesEqual(currentFiles, spec.Files) {
		httpx.WriteJSON(w, http.StatusOK, map[string]any{
			"files":           defaultAppFiles(cloneAppFiles(spec.Files)),
			"already_current": true,
		})
		return
	}

	op, err := s.store.CreateOperation(model.Operation{
		TenantID:            app.TenantID,
		Type:                model.OperationTypeDeploy,
		RequestedByType:     principal.ActorType,
		RequestedByID:       principal.ActorID,
		AppID:               app.ID,
		DesiredSpec:         &spec,
		DesiredSource:       source,
		DesiredOriginSource: model.AppOriginSource(app),
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	s.appendAudit(principal, "app.files.delete", "operation", op.ID, app.TenantID, map[string]string{"app_id": app.ID})
	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{
		"files":     defaultAppFiles(cloneAppFiles(spec.Files)),
		"operation": sanitizeOperationForAPI(op),
	})
}

func (s *Server) handleRestartApp(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.deploy") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.deploy scope")
		return
	}
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}
	if app.Spec.Replicas <= 0 {
		httpx.WriteError(w, http.StatusBadRequest, "disabled app cannot be restarted")
		return
	}

	spec, source, err := s.recoverAppDeployBaseline(app)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if strings.TrimSpace(spec.Image) == "" {
		httpx.WriteError(w, http.StatusConflict, "app has no deployable image; rebuild or re-import before restarting")
		return
	}
	spec.RestartToken = model.NewID("restart")
	op, err := s.store.CreateOperation(model.Operation{
		TenantID:            app.TenantID,
		Type:                model.OperationTypeDeploy,
		RequestedByType:     principal.ActorType,
		RequestedByID:       principal.ActorID,
		AppID:               app.ID,
		DesiredSpec:         &spec,
		DesiredSource:       source,
		DesiredOriginSource: model.AppOriginSource(app),
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	s.appendAudit(principal, "app.restart", "operation", op.ID, app.TenantID, map[string]string{"app_id": app.ID})
	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{
		"operation":     sanitizeOperationForAPI(op),
		"restart_token": spec.RestartToken,
	})
}

func applyEnvPatch(current map[string]string, set map[string]string, deleted []string) (map[string]string, bool, error) {
	if len(set) == 0 && len(deleted) == 0 {
		return nil, false, fmt.Errorf("set or delete is required")
	}
	env := cloneStringMap(current)
	if env == nil {
		env = map[string]string{}
	}

	deleteSet := make(map[string]struct{}, len(deleted))
	for _, rawKey := range deleted {
		key := strings.TrimSpace(rawKey)
		if key == "" {
			return nil, false, fmt.Errorf("delete contains empty key")
		}
		deleteSet[key] = struct{}{}
	}
	for rawKey := range set {
		key := strings.TrimSpace(rawKey)
		if key == "" {
			return nil, false, fmt.Errorf("set contains empty key")
		}
		if _, ok := deleteSet[key]; ok {
			return nil, false, fmt.Errorf("same env key cannot appear in both set and delete")
		}
	}

	changed := false
	for key := range deleteSet {
		if _, ok := env[key]; ok {
			delete(env, key)
			changed = true
		}
	}
	for rawKey, value := range set {
		key := strings.TrimSpace(rawKey)
		if currentValue, ok := env[key]; !ok || currentValue != value {
			env[key] = value
			changed = true
		}
	}
	if len(env) == 0 {
		env = nil
	}
	return env, changed, nil
}

func normalizeUploadedFiles(files []model.AppFile) ([]model.AppFile, error) {
	if len(files) == 0 {
		return nil, fmt.Errorf("files is required")
	}
	seen := make(map[string]struct{}, len(files))
	out := make([]model.AppFile, 0, len(files))
	for index := range files {
		file := files[index]
		file.Path = strings.TrimSpace(file.Path)
		if file.Path == "" {
			return nil, fmt.Errorf("files[%s].path is required", strconv.Itoa(index))
		}
		if !strings.HasPrefix(file.Path, "/") {
			return nil, fmt.Errorf("files[%s].path must be absolute", strconv.Itoa(index))
		}
		if file.Path == "/" || strings.HasSuffix(file.Path, "/") {
			return nil, fmt.Errorf("files[%s].path must point to a file", strconv.Itoa(index))
		}
		if _, ok := seen[file.Path]; ok {
			return nil, fmt.Errorf("duplicate file path %s", file.Path)
		}
		seen[file.Path] = struct{}{}
		if file.Mode == 0 {
			if file.Secret {
				file.Mode = 0o600
			} else {
				file.Mode = 0o644
			}
		}
		out = append(out, file)
	}
	return out, nil
}

func normalizeDeletedFilePaths(paths []string) ([]string, error) {
	if len(paths) == 0 {
		return nil, fmt.Errorf("at least one path query parameter is required")
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(paths))
	for _, rawPath := range paths {
		filePath := strings.TrimSpace(rawPath)
		if filePath == "" {
			return nil, fmt.Errorf("path query parameter must not be empty")
		}
		if !strings.HasPrefix(filePath, "/") {
			return nil, fmt.Errorf("path must be absolute")
		}
		if _, ok := seen[filePath]; ok {
			continue
		}
		seen[filePath] = struct{}{}
		out = append(out, filePath)
	}
	sort.Strings(out)
	return out, nil
}

func upsertAppFiles(current []model.AppFile, updates []model.AppFile) ([]model.AppFile, bool) {
	files := cloneAppFiles(current)
	indexByPath := make(map[string]int, len(files))
	for index, file := range files {
		indexByPath[file.Path] = index
	}

	changed := false
	for _, file := range updates {
		if index, ok := indexByPath[file.Path]; ok {
			if !appFileEqual(files[index], file) {
				files[index] = file
				changed = true
			}
			continue
		}
		indexByPath[file.Path] = len(files)
		files = append(files, file)
		changed = true
	}
	return files, changed
}

func deleteAppFiles(current []model.AppFile, deleted []string) ([]model.AppFile, bool) {
	if len(current) == 0 || len(deleted) == 0 {
		return cloneAppFiles(current), false
	}
	deleteSet := make(map[string]struct{}, len(deleted))
	for _, path := range deleted {
		deleteSet[path] = struct{}{}
	}
	out := make([]model.AppFile, 0, len(current))
	changed := false
	for _, file := range current {
		if _, ok := deleteSet[file.Path]; ok {
			changed = true
			continue
		}
		out = append(out, file)
	}
	if len(out) == 0 {
		return nil, changed
	}
	return out, changed
}

func appFilesEqual(left, right []model.AppFile) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if !appFileEqual(left[index], right[index]) {
			return false
		}
	}
	return true
}

func appFileEqual(left, right model.AppFile) bool {
	return left.Path == right.Path &&
		left.Content == right.Content &&
		left.Secret == right.Secret &&
		left.Mode == right.Mode
}

func appPersistentStorageEqual(left, right *model.AppPersistentStorageSpec) bool {
	switch {
	case left == nil || right == nil:
		return left == nil && right == nil
	case left.StoragePath != right.StoragePath:
		return false
	case left.StorageSize != right.StorageSize:
		return false
	case left.StorageClassName != right.StorageClassName:
		return false
	case left.ResetToken != right.ResetToken:
		return false
	}

	if len(left.Mounts) != len(right.Mounts) {
		return false
	}

	for index := range left.Mounts {
		if !appPersistentStorageMountEqual(left.Mounts[index], right.Mounts[index]) {
			return false
		}
	}

	return true
}

func appPersistentStorageMountEqual(left, right model.AppPersistentStorageMount) bool {
	return left.Kind == right.Kind &&
		left.Path == right.Path &&
		left.SeedContent == right.SeedContent &&
		left.Secret == right.Secret &&
		left.Mode == right.Mode
}

func defaultStringMap(in map[string]string) map[string]string {
	if in == nil {
		return map[string]string{}
	}
	return in
}

func defaultAppFiles(files []model.AppFile) []model.AppFile {
	if files == nil {
		return []model.AppFile{}
	}
	return files
}
