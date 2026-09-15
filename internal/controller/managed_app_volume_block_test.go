package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func volumeBlockFixture(now time.Time) (kubePod, kubeEvent) {
	var pod kubePod
	pod.Metadata.Name = "sample-pod"
	pod.Metadata.CreationTimestamp = now.Add(-time.Minute)
	pod.ObservedUID = "current-pod"
	pod.Spec.NodeName = "node-a"
	pod.Status.Phase = "Pending"
	event := kubeEvent{Reason: "FailedAttachVolume", Message: "volume provider reports missing target node", LastTimestamp: now.Format(time.RFC3339Nano)}
	event.InvolvedObject.Kind = "Pod"
	event.InvolvedObject.Name = pod.Metadata.Name
	event.InvolvedObject.UID = pod.ObservedUID
	return pod, event
}

func TestVolumeBlockRequiresCurrentUnstartedPodAndRecentEvidence(t *testing.T) {
	now := time.Now().UTC()
	for _, tc := range []struct {
		name   string
		modify func(*kubePod, *kubeEvent)
		want   bool
	}{
		{"attach error", func(*kubePod, *kubeEvent) {}, true},
		{"mount error", func(_ *kubePod, e *kubeEvent) { e.Reason = "FailedMount" }, true},
		{"previous pod", func(_ *kubePod, e *kubeEvent) { e.InvolvedObject.UID = "previous-pod" }, false},
		{"missing uid", func(p *kubePod, _ *kubeEvent) { p.ObservedUID = "" }, false},
		{"old warning", func(_ *kubePod, e *kubeEvent) { e.LastTimestamp = now.Add(-6 * time.Minute).Format(time.RFC3339Nano) }, false},
		{"unrelated warning", func(_ *kubePod, e *kubeEvent) { e.Reason = "FailedScheduling" }, false},
		{"recovered", func(p *kubePod, _ *kubeEvent) { p.Status.Phase = "Running" }, false},
		{"terminating", func(p *kubePod, _ *kubeEvent) { p.Metadata.DeletionTimestamp = now.Format(time.RFC3339Nano) }, false},
		{"already started init", func(p *kubePod, _ *kubeEvent) {
			p.Status.InitContainerStatuses = []kubeContainerStatus{{State: kubeRuntimeState{Terminated: &kubeStateDetail{ExitCode: 0}}}}
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod, event := volumeBlockFixture(now)
			tc.modify(&pod, &event)
			got := podVolumeMountFailureMessage(pod, []kubeEvent{event}, now)
			if (got != "") != tc.want {
				t.Fatalf("message=%q, want evidence=%v", got, tc.want)
			}
		})
	}
}

func TestVolumeBlockGraceResetsAfterRecoveryOrReplacement(t *testing.T) {
	now := time.Now().UTC()
	tracker := rolloutVolumeBlockTracker{}
	if err := tracker.observe(now, "pod-a/uid-a", "attach blocked"); err != nil {
		t.Fatal(err)
	}
	if err := tracker.observe(now.Add(time.Minute), "pod-a/uid-a", "attach blocked"); err != nil {
		t.Fatal(err)
	}
	if err := tracker.observe(now.Add(90*time.Second), "", ""); err != nil {
		t.Fatal(err)
	}
	if err := tracker.observe(now.Add(3*time.Minute), "pod-a/uid-a", "new failure"); err != nil {
		t.Fatal(err)
	}
	if err := tracker.observe(now.Add(4*time.Minute), "pod-a/uid-b", "replacement pod"); err != nil {
		t.Fatal(err)
	}
	if err := tracker.observe(now.Add(5*time.Minute), "pod-a/uid-b", "replacement pod"); err != nil {
		t.Fatal(err)
	}
	if err := tracker.observe(now.Add(6*time.Minute), "pod-a/uid-b", "replacement pod"); err == nil || !strings.Contains(err.Error(), "replacement pod") {
		t.Fatalf("expected bounded failure: %v", err)
	}
}

func TestDeploymentVolumeBlockReadsEventsOnlyForCurrentPendingPods(t *testing.T) {
	now := time.Now().UTC()
	pod, event := volumeBlockFixture(now)
	reads := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reads++
		if r.URL.Query().Get("fieldSelector") != "involvedObject.name=sample-pod,involvedObject.kind=Pod" {
			t.Errorf("unexpected selector %s", r.URL.RawQuery)
		}
		_ = json.NewEncoder(w).Encode(kubeEventList{Items: []kubeEvent{event}})
	}))
	defer server.Close()
	client := &kubeClient{client: server.Client(), baseURL: server.URL}
	identity, message := deploymentVolumeBlockMessage(context.Background(), client, "tenant-demo", []kubePod{pod}, kubeDeployment{}, now)
	if identity != "sample-pod/current-pod" || message == "" || reads != 1 {
		t.Fatalf("identity=%q message=%q reads=%d", identity, message, reads)
	}
	pod.Status.Phase = "Running"
	identity, _ = deploymentVolumeBlockMessage(context.Background(), client, "tenant-demo", []kubePod{pod}, kubeDeployment{}, now)
	if identity != "" || reads != 1 {
		t.Fatal("recovered pod caused event reads")
	}
}
