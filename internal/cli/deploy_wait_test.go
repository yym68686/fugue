package cli

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestWaitForOperationsToleratesTransientGatewayError(t *testing.T) {
	previousPollInterval := deployWaitPollInterval
	deployWaitPollInterval = time.Millisecond
	defer func() {
		deployWaitPollInterval = previousPollInterval
	}()

	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/operations/op_123" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
		attempts++
		if attempts == 1 {
			http.Error(w, "error code: 502", http.StatusBadGateway)
			return
		}
		_, _ = w.Write([]byte(`{"operation":{"id":"op_123","tenant_id":"tenant_123","app_id":"app_123","type":"deploy","status":"completed","execution_mode":"managed","requested_by_type":"api-key","requested_by_id":"key_123","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
	}))
	defer server.Close()

	client, err := newClientWithOptions(server.URL, "token", clientOptions{
		RequireToken:   true,
		ReadRetryCount: -1,
		RequestTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cli := newCLI(&stdout, &stderr)
	final, err := cli.waitForOperations(client, []model.Operation{{ID: "op_123", Status: model.OperationStatusPending}})
	if err != nil {
		t.Fatalf("wait for operations: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("expected retry after transient gateway error, got %d attempts", attempts)
	}
	if len(final) != 1 || final[0].ID != "op_123" || final[0].Status != model.OperationStatusCompleted {
		t.Fatalf("unexpected final operations %+v", final)
	}
	if !bytes.Contains(stderr.Bytes(), []byte("temporarily unavailable; continuing to poll")) {
		t.Fatalf("expected transient warning, got stderr=%q", stderr.String())
	}
}
