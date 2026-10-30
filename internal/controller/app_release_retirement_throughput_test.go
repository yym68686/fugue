package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
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

func TestRetirementPriorityKeepsIndependentFairResumePositions(t *testing.T) {
	var releases []model.AppRelease
	var pods []map[string]any
	for i := 0; i < 12; i++ {
		r := model.AppRelease{ID: fmt.Sprint(i), CreatedAt: time.Unix(int64(i), 0), Role: model.AppReleaseRoleCandidate, Status: model.AppReleaseStatusFailed, RevisionWorkload: &model.AppReleaseWorkload{DeploymentName: fmt.Sprint(i)}}
		releases = append(releases, r)
		if i < 8 {
			pods = append(pods, map[string]any{"metadata": map[string]any{"name": r.ID, "labels": map[string]any{runtime.FugueLabelAppReleaseID: r.ID}}, "spec": map[string]any{"nodeName": "worker"}})
		}
	}
	inv := indexReleaseDrainInventory(nil, pods)
	queue := prioritizeAppReleaseRetirement(releases, [3]appReleaseRetirementPosition{}, inv)
	if len(queue) != 12 {
		t.Fatal("lost retirement candidates")
	}
	for i, want := range []string{"0", "1", "2", "3", "8", "4", "5", "6", "7", "9", "10", "11"} {
		if queue[i].release.ID != want {
			t.Fatalf("position %d: %s want %s", i, queue[i].release.ID, want)
		}
	}
	var progress [3]appReleaseRetirementPosition
	progress[0], progress[1] = appReleaseRetirementPositionOf(releases[3]), appReleaseRetirementPositionOf(releases[8])
	next := prioritizeAppReleaseRetirement(releases, progress, inv)
	if next[0].release.ID != "4" || next[4].release.ID != "9" {
		t.Fatal("one priority class reset another class's progress")
	}
}

func TestRetirementQueueRunsOneHundredAttemptsAndJoinsOnCancellation(t *testing.T) {
	for _, cancelEarly := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelEarly), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var queue []prioritizedReleaseRetirement
			for i := 0; i < 130; i++ {
				queue = append(queue, prioritizedReleaseRetirement{release: model.AppRelease{ID: fmt.Sprint(i)}})
			}
			started := make(chan struct{}, 130)
			gate := make(chan struct{})
			done := make(chan error, 1)
			var active, peak, completed atomic.Int32
			var positions [3]appReleaseRetirementPosition
			go func() {
				done <- runAppReleaseRetirementQueue(ctx, queue, &positions, func(ctx context.Context, _ prioritizedReleaseRetirement) {
					n := active.Add(1)
					for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
					}
					started <- struct{}{}
					select {
					case <-gate:
					case <-ctx.Done():
					}
					active.Add(-1)
					completed.Add(1)
				})
			}()
			timer := time.NewTimer(3 * time.Second)
			defer timer.Stop()
			for i := 0; i < 100; i++ {
				select {
				case <-started:
				case <-timer.C:
					t.Fatal("did not launch requested 100 concurrent attempts")
				}
			}
			if active.Load() != 100 || peak.Load() != 100 {
				t.Fatal("incorrect concurrency", active.Load(), peak.Load())
			}
			if cancelEarly {
				cancel()
			} else {
				close(gate)
			}
			select {
			case err := <-done:
				if !cancelEarly && err != nil {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("worker did not join outstanding attempts")
			}
			if active.Load() != 0 || peak.Load() > 100 || !cancelEarly && completed.Load() != 130 {
				t.Fatal("leaked or dropped retirement attempts")
			}
		})
	}
}
