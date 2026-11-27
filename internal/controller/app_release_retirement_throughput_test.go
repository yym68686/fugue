package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestWithdrawnUnreadyRuntimeStillRequiresPositiveStableDrain(t *testing.T) {
	for _, scenario := range []string{"unready_idle", "waiting_idle", "busy", "agent_unready", "missing_identity", "restarted", "new_owned_pod"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			s, app, release, f := newDrainWorkloadFixtureWithOutcome(t, true)
			status := objectMapField(f.deployment, "status")
			status["readyReplicas"], status["availableReplicas"], status["unavailableReplicas"] = 0, 0, 2
			for _, pod := range f.pods {
				c := mapSlice(nestedObjectValue(pod, "status", "containerStatuses"))[0]
				c["ready"] = false
				if scenario == "waiting_idle" {
					c["state"] = map[string]any{"waiting": map[string]any{"reason": "CrashLoopBackOff"}}
				}
			}
			switch scenario {
			case "busy":
				f.active = 1
			case "agent_unready":
				mapSlice(nestedObjectValue(f.pods[0], "status", "initContainerStatuses"))[0]["ready"] = false
			case "missing_identity":
				mapSlice(nestedObjectValue(f.pods[0], "status", "containerStatuses"))[0]["containerID"] = ""
			case "restarted":
				f.change = func(f *drainWorkloadFixture) {
					mapSlice(nestedObjectValue(f.pods[0], "status", "containerStatuses"))[0]["restartCount"] = 1
				}
			case "new_owned_pod":
				f.change = func(f *drainWorkloadFixture) {
					f.change = nil
					pod := cloneKubeMap(f.pods[0])
					m := objectMapField(pod, "metadata")
					m["name"], m["uid"] = "late-pod", "late-uid"
					f.pods = append(f.pods, pod)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			metrics, err := (kubeSafeRolloutDrainObserver{service: s}).QuerySafeRolloutDrainMetrics(ctx, app, release, time.Now())
			want := scenario == "unready_idle" || scenario == "waiting_idle"
			if want && (err != nil || !metrics.Ready || metrics.FinalCount != 2) {
				t.Fatalf("positive drain unavailable for unhealthy withdrawn runtime: %+v %v", metrics, err)
			}
			if !want && (err == nil || metrics.Ready) {
				t.Fatalf("accepted unsafe %s runtime: %+v %v", scenario, metrics, err)
			}
		})
	}
}

func TestReleaseDrainMetadataHydratesOnlyOwnedPodsAndRechecksVersions(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprint(changed), func(t *testing.T) {
			s, app, release, f := newDrainWorkloadFixtureWithOutcome(t, true)
			metadataLists, ownedReads := 0, 0
			unrelated := cloneKubeMap(f.pods[0])
			m := objectMapField(unrelated, "metadata")
			m["name"], m["uid"], m["labels"], m["ownerReferences"] = "unrelated", "foreign", map[string]any{}, nil
			f.pods = append(f.pods, unrelated)
			f.extra = func(w http.ResponseWriter, r *http.Request) bool {
				if strings.HasSuffix(r.URL.Path, "/pods") && r.Header.Get("Accept") == metadataListAccept {
					metadataLists++
					items := []map[string]any{}
					for _, p := range f.pods {
						items = append(items, map[string]any{"metadata": p["metadata"]})
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"items": items})
					return true
				}
				if strings.Contains(r.URL.Path, "/pods/") && !strings.Contains(r.URL.Path, "/proxy/") {
					name := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
					for _, p := range f.pods {
						if objectStringField(objectMapField(p, "metadata"), "name") == name {
							if name == "unrelated" {
								t.Error("hydrated unrelated executable spec")
							}
							ownedReads++
							full := cloneKubeMap(p)
							if changed {
								objectMapField(full, "metadata")["resourceVersion"] = "changed"
							}
							_ = json.NewEncoder(w).Encode(full)
							return true
						}
					}
				}
				return false
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			metrics, err := (kubeSafeRolloutDrainObserver{service: s}).QuerySafeRolloutDrainMetrics(ctx, app, release, time.Now())
			if !changed && (err != nil || !metrics.Ready || metadataLists != 1 || ownedReads != 2) {
				t.Fatalf("metadata recheck: lists=%d reads=%d err=%v", metadataLists, ownedReads, err)
			}
			if changed && (err == nil || metrics.Ready) {
				t.Fatal("accepted Pod changed after full ownership metadata snapshot")
			}
		})
	}
}
