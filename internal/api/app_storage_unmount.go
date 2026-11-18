package api

import (
	"net/http"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/runtime"
)

func (s *Server) planAppStorageUnmount(app model.App, mountPath string) (model.AppStorageUnmountPlan, model.AppSpec, error) {
	spec, retained, err := model.AppSpecWithoutPersistentMount(app.Spec, mountPath)
	plan := model.AppStorageUnmountPlan{
		AppID: app.ID, Path: mountPath, SpecSHA256: model.AppSpecSHA256(app.Spec),
		RetainedMounts: retained,
	}
	if err != nil {
		plan.Reason = err.Error()
		return plan, model.AppSpec{}, nil
	}
	rt, err := s.store.GetRuntime(app.Spec.RuntimeID)
	if err != nil {
		return plan, model.AppSpec{}, err
	}
	if !model.RuntimeIsInternal(rt) || rt.Type != model.RuntimeTypeManagedShared {
		plan.Reason = "online unmount requires a managed Kubernetes runtime"
		return plan, model.AppSpec{}, nil
	}
	plan.Allowed = true
	plan.ReusePVC = true
	plan.ClaimName = runtime.PersistentStoragePVCName(app, *app.Spec.PersistentStorage)
	plan.Reason = "specification is eligible; controller must verify serving workload, same-node capacity and live PVC before applying"
	return plan, spec, nil
}

func (s *Server) handlePlanAppStorageUnmount(w http.ResponseWriter, r *http.Request) {
	p := mustPrincipal(r)
	if !p.IsPlatformAdmin() && !p.HasScope("app.read") && !p.HasScope("app.deploy") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.read or app.deploy scope")
		return
	}
	app, ok := s.loadAuthorizedApp(w, r, p)
	if !ok {
		return
	}
	plan, _, err := s.planAppStorageUnmount(app, r.URL.Query().Get("path"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	w.Header().Set("ETag", `"`+plan.SpecSHA256+`"`)
	httpx.WriteJSON(w, http.StatusOK, plan)
}

func (s *Server) handleExecuteAppStorageUnmount(w http.ResponseWriter, r *http.Request) {
	p := mustPrincipal(r)
	if !p.IsPlatformAdmin() && !p.HasScope("app.deploy") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.deploy scope")
		return
	}
	hash, ok := appSpecPrecondition(w, r)
	if !ok {
		return
	}
	if hash == "" {
		httpx.WriteError(w, http.StatusBadRequest, "If-Match from an unmount plan is required")
		return
	}
	app, ok := s.loadAuthorizedApp(w, r, p)
	if !ok {
		return
	}
	if model.AppSpecSHA256(app.Spec) != hash {
		httpx.WriteError(w, http.StatusPreconditionFailed, "app intent changed; refresh the unmount plan")
		return
	}
	var req model.AppStorageUnmountRequest
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, "invalid unmount request")
		return
	}
	plan, spec, err := s.planAppStorageUnmount(app, req.Path)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if !plan.Allowed {
		httpx.WriteError(w, http.StatusConflict, plan.Reason)
		return
	}
	if err := req.Validate(app.Spec); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	op, err := s.store.CreateOperationForAppSpec(model.Operation{
		Type: model.OperationTypeAppStorageUnmount, TenantID: app.TenantID, AppID: app.ID,
		RequestedByType: p.ActorType, RequestedByID: p.ActorID, DesiredSpec: &spec, StorageUnmount: &req,
	}, hash)
	if err != nil {
		s.writeAppPreconditionError(w, err, hash)
		return
	}
	s.appendAudit(p, "app.storage.unmount", "operation", op.ID, app.TenantID, map[string]string{"app_id": app.ID, "path": req.Path, "claim_name": plan.ClaimName})
	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{"operation": sanitizeOperationForAPI(op)})
}
