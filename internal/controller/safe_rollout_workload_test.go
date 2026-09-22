package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

func TestSafeRolloutBindsVerifiedRevisionAndPreservesCanonicalIdentity(t *testing.T) {
	for _, scenario := range []string{"valid", "defaults", "omitted_zero_delay", "omitted_nonzero_delay", "wrong_owner", "wrong_release", "wrong_uid", "wrong_selector", "extra_selector", "wrong_image", "wrong_key", "wrong_service_port", "missing_resource", "deleting", "replaced_between_reads", "generation_changed", "wrong_operation", "operation_finished"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			st, _, app, op := newSafeRolloutTestState(t)
			op, claimed, err := st.TryClaimPendingOperation(op.ID)
			if err != nil || !claimed {
				t.Fatal("claim", err)
			}
			app.Spec.RuntimeID = "runtime"
			svc := &Service{Store: st, Renderer: runtime.Renderer{StrictDrain: runtime.DefaultStrictDrainConfig()}}
			app = svc.Renderer.PrepareApp(app)
			release, err := st.CreateAppRelease(model.AppRelease{ID: "candidate", TenantID: app.TenantID, AppID: app.ID, Role: model.AppReleaseRoleCandidate, Status: model.AppReleaseStatusCreating, RuntimeID: app.Spec.RuntimeID, ResolvedImageRef: app.Spec.Image, SpecSnapshot: cloneControllerAppSpec(&app.Spec)})
			if err != nil {
				t.Fatal(err)
			}
			revision := safeRolloutCandidateRevision(release.ID)
			options := runtime.RenderOptions{StrictDrain: svc.Renderer.StrictDrain, Revision: revision}
			release.DeploymentName = runtime.RuntimeAppResourceNameWithOptions(app, options)
			release.ServiceName = runtime.RuntimeAppServiceNameWithOptions(app, options)
			release, err = st.UpdateAppRelease(release)
			if err != nil {
				t.Fatal(err)
			}
			objects := svc.Renderer.BuildManagedAppRevisionChildObjects(app, runtime.SchedulingConstraints{}, nil, nil, revision)
			live := map[string]map[string]any{}
			for _, o := range objects {
				if o["kind"] == "Deployment" || o["kind"] == "Service" {
					obj := cloneKubeMap(o)
					m := objectMapField(obj, "metadata")
					m["uid"] = strings.ToLower(o["kind"].(string)) + "-uid"
					m["generation"] = float64(1)
					live[o["kind"].(string)] = obj
				}
			}
			dep, service := live["Deployment"], live["Service"]
			switch scenario {
			case "omitted_zero_delay", "omitted_nonzero_delay":
				for _, obj := range objects {
					if obj["kind"] == "Deployment" {
						probe := objectMapField(mapSlice(nestedObjectValue(obj, "spec", "template", "spec", "containers"))[0], "readinessProbe")
						probe["initialDelaySeconds"] = 0
						if scenario == "omitted_nonzero_delay" {
							probe["initialDelaySeconds"] = 5
						}
					}
				}
				delete(objectMapField(mapSlice(nestedObjectValue(dep, "spec", "template", "spec", "containers"))[0], "readinessProbe"), "initialDelaySeconds")
			case "defaults":
				c := mapSlice(nestedObjectValue(dep, "spec", "template", "spec", "containers"))[0]
				c["terminationMessagePath"] = "/dev/termination-log"
				c["terminationMessagePolicy"] = "File"
				for _, p := range mapSlice(nestedObjectValue(service, "spec", "ports")) {
					p["protocol"] = "TCP"
				}
			case "wrong_owner":
				objectMapField(objectMapField(dep, "metadata"), "labels")[runtime.FugueLabelTenantID] = "other"
			case "wrong_release":
				objectMapField(objectMapField(service, "metadata"), "labels")[runtime.FugueLabelAppReleaseID] = "other"
			case "wrong_uid":
				objectMapField(dep, "metadata")["uid"] = ""
			case "wrong_selector":
				objectMapField(objectMapField(service, "spec"), "selector")[runtime.FugueLabelAppWorkload] = "canonical"
			case "extra_selector":
				objectMapField(objectMapField(service, "spec"), "selector")["extra"] = "unexpected"
			case "wrong_image":
				mapSlice(nestedObjectValue(dep, "spec", "template", "spec", "containers"))[0]["image"] = "registry.example/wrong:v2"
			case "wrong_key":
				objectMapField(objectMapField(dep, "metadata"), "annotations")[runtime.FugueAnnotationReleaseKey] = "other"
			case "wrong_service_port":
				mapSlice(nestedObjectValue(service, "spec", "ports"))[0]["targetPort"] = float64(9999)
			case "deleting":
				objectMapField(dep, "metadata")["deletionTimestamp"] = "2026-01-01T00:00:00Z"
			case "wrong_operation":
				op.AppID = "other"
			case "operation_finished":
				if _, err := st.FailOperation(op.ID, "synthetic cancellation"); err != nil {
					t.Fatal(err)
				}
			}
			reads := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Error("binding mutated Kubernetes")
					http.Error(w, "invalid", 400)
					return
				}
				reads++
				if scenario == "missing_resource" {
					http.NotFound(w, r)
					return
				}
				if reads == 3 {
					if scenario == "replaced_between_reads" {
						objectMapField(dep, "metadata")["uid"] = "replacement"
					}
					if scenario == "generation_changed" {
						objectMapField(dep, "metadata")["generation"] = float64(2)
					}
				}
				if strings.Contains(r.URL.Path, "/deployments/") {
					_ = json.NewEncoder(w).Encode(dep)
				} else {
					_ = json.NewEncoder(w).Encode(service)
				}
			}))
			defer server.Close()
			client := &kubeClient{baseURL: server.URL, client: server.Client()}
			state := &safeRolloutState{Enabled: true, CandidateApp: app, Candidate: release}
			err = svc.bindSafeRolloutWorkload(context.Background(), client, op, state, objects)
			valid := scenario == "valid" || scenario == "defaults" || scenario == "omitted_zero_delay"
			if !valid {
				if err == nil {
					t.Fatal("invalid binding accepted")
				}
				stored, _ := st.GetAppRelease(app.TenantID, true, release.ID)
				if stored.RevisionWorkload != nil {
					t.Fatal("rejected identity was written")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			binding := state.Candidate.RevisionWorkload
			if binding == nil || binding.OperationID != op.ID || binding.DeploymentUID != "deployment-uid" || binding.ServiceUID != "service-uid" || binding.BoundAt.IsZero() {
				t.Fatalf("incomplete binding: %+v", binding)
			}
			aligned := svc.safeRolloutApplyCanonicalStableFields(context.Background(), app, state.Candidate, "aligned")
			aligned, err = st.UpdateAppRelease(aligned)
			if err != nil {
				t.Fatal(err)
			}
			if aligned.DeploymentName == binding.DeploymentName || !reflect.DeepEqual(aligned.RevisionWorkload, binding) {
				t.Fatal("canonical alignment rewrote immutable revision")
			}
			changed := app
			changed.Spec = *cloneControllerAppSpec(&app.Spec)
			changed.Spec.Env = map[string]string{"CONFIG_VERSION": "new"}
			if !svc.safeRolloutBoundReleaseMatchesApp(app, aligned) || svc.safeRolloutBoundReleaseMatchesApp(changed, aligned) {
				t.Fatal("bound execution comparison did not distinguish configuration with the same image")
			}
			// Starting a later operation must retain the previously verified
			// stable revision even if the business AppSpec has since changed.
			principal := model.Principal{TenantID: app.TenantID, ActorType: model.ActorTypeSystem, ActorID: "safe-rollout-controller"}
			baseline, err := svc.ensureSafeRolloutCanonicalStableBaseline(context.Background(), op, changed, principal)
			if err != nil || !reflect.DeepEqual(baseline, aligned) {
				t.Fatal("next baseline rewrote bound stable", err)
			}
			mismatch := &safeRolloutState{Enabled: true, StableAlignmentAllowed: true, CandidateApp: changed, Candidate: aligned}
			if ok, err := svc.alignSafeRolloutPromotedStableRelease(context.Background(), op, mismatch); err == nil || ok {
				t.Fatal("promoted alignment accepted different intent")
			}
			aligned.Status = model.AppReleaseStatusServing
			aligned, err = st.UpdateAppRelease(aligned)
			if err != nil {
				t.Fatal(err)
			}
			priorReads := reads
			if err := svc.reconcileServingReleaseCanonicalTargetIfReady(context.Background(), client, runtime.NamespaceForTenant(app.TenantID), changed); err != nil || reads != priorReads {
				t.Fatal("mismatched reconciler should retain target without probing canonical", err)
			}
			retained, err := st.GetAppRelease(app.TenantID, true, aligned.ID)
			if err != nil || !reflect.DeepEqual(retained, aligned) {
				t.Fatal("mismatched reconciliation changed release", err)
			}
			changed.Spec.RuntimeID, changed.Spec.Image = "new-runtime", "registry.example/app:v3"
			preserved := svc.safeRolloutApplyCanonicalStableFields(context.Background(), changed, aligned, "reconcile")
			if preserved.SourceRef != aligned.SourceRef || preserved.ResolvedImageRef != aligned.ResolvedImageRef || preserved.RuntimeID != aligned.RuntimeID || !reflect.DeepEqual(preserved.SpecSnapshot, aligned.SpecSnapshot) {
				t.Fatal("canonical alignment overwrote bound executable intent")
			}
			if err := svc.bindSafeRolloutWorkload(context.Background(), client, op, state, objects); err != nil {
				t.Fatal("idempotent retry", err)
			}
			state.Candidate = aligned
			if err := svc.verifyResumedSafeRolloutWorkload(context.Background(), client, op, state, objects); err != nil {
				t.Fatal("resumed canonical verification", err)
			}
			if state.Candidate.DeploymentName != aligned.DeploymentName {
				t.Fatal("resumed verification rewrote serving target")
			}
			objectMapField(dep, "metadata")["uid"] = "replacement"
			if err := svc.verifyResumedSafeRolloutWorkload(context.Background(), client, op, state, objects); err == nil {
				t.Fatal("resumed verification accepted replacement UID")
			}
		})
	}
}
