package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/edgegroupfront"
	"fugue/internal/releaseguardian"
)

func TestNodeWorkerShadowOnlyPermitsScopedObservations(t *testing.T) {
	reader := &observationTestReader{}
	exec := nodeWorkerShadowExec{next: reader, namespace: "fixture-system"}
	for _, tc := range []struct {
		namespace, container string
		command              []string
	}{
		{"other", "edge-front", []string{"cat", edgegroupfront.NativeStatePath}},
		{exec.namespace, "edge", []string{"cat", edgegroupfront.NativeStatePath}},
		{exec.namespace, "edge-front", []string{"cat", "/other"}},
		{exec.namespace, "edge-front", []string{frontActivationCASBinary, "--worker-recipient", "ignored"}},
		{exec.namespace, "edge-front", []string{frontActivationCASBinary, "--worker-convergence"}},
	} {
		if _, err := exec.Exec(context.Background(), tc.namespace, "fixture-front", tc.container, tc.command...); err == nil {
			t.Fatalf("accepted unrelated exec: %+v", tc)
		}
	}
	if reader.calls != 0 {
		t.Fatal("an unrelated command reached the executor")
	}
	if _, err := exec.Exec(context.Background(), exec.namespace, "fixture-front", "edge-front", "cat", edgegroupfront.NativeStatePath); err != nil {
		t.Fatal(err)
	}
	if _, err := exec.Exec(context.Background(), exec.namespace, "fixture-front", "edge-front", frontActivationCASBinary, "--worker-recipient"); err != nil {
		t.Fatal(err)
	}
	if reader.calls != 2 {
		t.Fatalf("expected two observations, got %d", reader.calls)
	}

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete, "upgrading-get"} {
		request := httptest.NewRequest(method, "/api/v1/namespaces/fixture-system/pods", nil)
		if method == "upgrading-get" {
			request.Method = http.MethodGet
			request.Header.Set("Upgrade", "SPDY/3.1")
		}
		// A nil next transport panics if a rejected request is forwarded.
		if _, err := (nodeWorkerShadowObjects{}).RoundTrip(request); err == nil {
			t.Fatal("accepted an object mutation or an upgraded transport:", method)
		}
	}
}

func TestNodeWorkerShadowFailsClosedOnRecipientDriftWithoutWrites(t *testing.T) {
	front, client, journal := nodeWorkerRuntimeFixture(t)
	front.now = func() time.Time { return journal.CreatedAt }
	exec := &observationTestReader{}
	front.executor = exec
	store, err := releaseguardian.NewAuthorityStore(client, front.config.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	report, err := front.shadowNodeWorker(context.Background(), store, releaseguardian.NodeWorkerComponent{}, journal)
	if err == nil || !strings.Contains(err.Error(), "exact running Front recipient") || report.Schema != "" || exec.calls != 1 {
		t.Fatalf("incorrect result for unproven recipient: %+v, %v, exec calls=%d", report, err, exec.calls)
	}
	for _, action := range client.Actions() {
		switch action.GetVerb() {
		case "get", "list":
		default:
			t.Fatalf("shadow attempted a mutation: %+v", action)
		}
	}
}
