package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

func TestDrainAgentImageActivationPreservesCurrentServingTemplate(t *testing.T) {
	for _, intent := range []string{"", model.AppRolloutIntentOnlineRestart} {
		t.Run("intent_"+intent, func(t *testing.T) {
			app := managedAppLiveGuardTestApp(nil)
			app.Spec.RolloutIntent = intent
			app.Spec.Continuity = &model.AppContinuityPolicy{ZeroDowntime: &model.AppZeroDowntimePolicy{Enabled: true, Mode: model.AppZeroDowntimeModeSafe, Strategy: model.AppZeroDowntimeStrategyStableCandidate}}
			svc := &Service{Renderer: runtime.Renderer{StrictDrain: runtime.DefaultStrictDrainConfig()}}
			app = svc.Renderer.PrepareApp(app)
			objects := svc.Renderer.BuildManagedAppChildObjects(app, runtime.SchedulingConstraints{}, nil)
			desired := firstManagedAppDeploymentObject(objects, runtime.RuntimeAppResourceName(app))
			live := cloneKubeMap(desired)
			found := false
			for _, c := range mapSlice(nestedObjectValue(live, "spec", "template", "spec", "initContainers")) {
				if c["name"] == "fugue-drain-agent" {
					c["image"] = "ghcr.io/example/drain:previous"
					found = true
				}
			}
			if !found {
				t.Fatal("fixture has no drain agent")
			}
			live["status"] = map[string]any{"replicas": 1, "updatedReplicas": 1, "readyReplicas": 1, "availableReplicas": 1, "observedGeneration": 1}
			objectMapField(live, "metadata")["generation"] = 1
			liveData, err := json.Marshal(live)
			if err != nil {
				t.Fatal(err)
			}
			var liveDeployment kubeDeployment
			if err := json.Unmarshal(liveData, &liveDeployment); err != nil {
				t.Fatal(err)
			}
			managed := managedAppLiveGuardObject(t, app, runtime.SchedulingConstraints{})
			managed.Status = runtime.ManagedAppStatus{Phase: runtime.ManagedAppPhaseReady, ReadyReplicas: 1}
			guardClient := managedAppLiveGuardClient(t, managed, liveDeployment, true, true, nil)
			if _, err := svc.prepareManagedAppReconcileRolloutWithEvidence(context.Background(), guardClient, managed.Metadata.Namespace, managed, app, "", runtime.SchedulingConstraints{}); err != nil {
				t.Fatalf("no-op helper update failed guard: %v", err)
			}
			client := &kubeClient{baseURL: "http://kube.test", client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) { return okJSONResponse(string(liveData)), nil })}}
			got := svc.preserveManagedAppServingDeploymentTemplate(context.Background(), client, runtime.NamespaceForTenant(app.TenantID), app, objects)
			prepared := firstManagedAppDeploymentObject(got, runtime.RuntimeAppResourceName(app))
			if !normalizedKubeValueEqual(nestedObjectValue(prepared, "spec", "template"), nestedObjectValue(live, "spec", "template")) {
				t.Fatal("same serving release would replace its Pods")
			}
			// A real restart has a different executable key and must render the
			// new helper; preserving a template must not swallow desired work.
			restarted := app
			restarted.Spec.RestartToken = "new-operation"
			restartObjects := svc.Renderer.BuildManagedAppChildObjects(restarted, runtime.SchedulingConstraints{}, nil)
			expected := cloneKubeMap(firstManagedAppDeploymentObject(restartObjects, runtime.RuntimeAppResourceName(restarted)))
			got = svc.preserveManagedAppServingDeploymentTemplate(context.Background(), client, runtime.NamespaceForTenant(app.TenantID), restarted, restartObjects)
			if !normalizedKubeValueEqual(firstManagedAppDeploymentObject(got, runtime.RuntimeAppResourceName(restarted)), expected) {
				t.Fatal("real restart was swallowed")
			}
		})
	}
}

func TestDrainAgentImageOnlyChangeDoesNotRelaxOtherLifecycleGuards(t *testing.T) {
	app := managedAppLiveGuardTestApp(nil)
	app.Spec.RolloutIntent = model.AppRolloutIntentOnlineRestart
	svc := &Service{Renderer: runtime.Renderer{StrictDrain: runtime.DefaultStrictDrainConfig()}}
	live, found := svc.expectedManagedAppDeployment(svc.Renderer.PrepareApp(app), runtime.SchedulingConstraints{})
	if !found {
		t.Fatal("missing expected deployment")
	}
	raw, _ := json.Marshal(live)
	for _, scenario := range []string{"image_only", "quiet_period", "missing_agent", "termination_grace"} {
		t.Run(scenario, func(t *testing.T) {
			var desired kubeDeployment
			_ = json.Unmarshal(raw, &desired)
			found := false
			for i := range desired.Spec.Template.Spec.InitContainers {
				c := &desired.Spec.Template.Spec.InitContainers[i]
				if c.Name == "fugue-drain-agent" {
					c.Image = "ghcr.io/example/drain:new"
					found = true
				}
			}
			if !found {
				t.Fatal("fixture has no drain agent")
			}
			switch scenario {
			case "quiet_period":
				desired.Spec.Template.Metadata.Annotations["fugue.io/drain-quiet-period-seconds"] = "10"
			case "missing_agent":
				desired.Spec.Template.Spec.InitContainers = nil
			case "termination_grace":
				grace := int64(900)
				desired.Spec.Template.Spec.TerminationGracePeriodSeconds = &grace
			}
			if got := managedDeploymentDrainAgentImageOnlyChanged(live, desired); got != (scenario == "image_only") {
				t.Fatalf("unexpected image-only decision: %t", got)
			}
		})
	}
}
