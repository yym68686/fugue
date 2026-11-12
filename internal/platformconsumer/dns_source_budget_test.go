package platformconsumer

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

type sourceBudgetTransport func(*http.Request) (*http.Response, error)

func (f sourceBudgetTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestDNSRouteSourceReadBudgets(t *testing.T) {
	for _, mode := range []string{"selected", "retained", "interactive", "caller_deadline"} {
		t.Run(mode, func(t *testing.T) {
			ctx := t.Context()
			want := 45 * time.Second
			if mode == "interactive" {
				want = 15 * time.Second
			}
			if mode == "caller_deadline" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, time.Second)
				defer cancel()
				want = time.Second
			}
			httpClient := &http.Client{Timeout: time.Millisecond, Transport: sourceBudgetTransport(func(req *http.Request) (*http.Response, error) {
				deadline, ok := req.Context().Deadline()
				remaining := time.Until(deadline)
				if !ok || remaining > want || remaining < want-time.Second/2 {
					t.Errorf("wrong bounded read budget: %v, want %v", remaining, want)
				}
				return nil, context.DeadlineExceeded
			})}
			client := Client{BaseURL: "https://control.example", HTTPClient: httpClient}
			var err error
			switch mode {
			case "retained":
				_, err = client.RetainedDNSRouteSources(ctx, Identity{}, model.PlatformConsumerAssignment{}, model.PlatformArtifactRelease{})
			case "interactive":
				err = client.GetJSON(ctx, "/v1/interactive", "scoped", &struct{}{})
			default:
				_, err = client.DNSRouteSources(ctx, Identity{}, model.PlatformConsumerAssignment{}, model.PlatformArtifactRelease{})
			}
			if !errors.Is(err, ErrUnavailable) || httpClient.Timeout != time.Millisecond || httpClient.CheckRedirect != nil {
				t.Fatal("request lost unavailable classification or mutated shared client", err)
			}
		})
	}
}

func TestDNSRouteSourceReadBeyondInteractiveBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("16-second complete source read regression")
	}
	a := model.PlatformConsumerAssignment{ArtifactID: "dns", ExpectedConsumerSetID: "selected"}
	r := model.PlatformArtifactRelease{ID: "release"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		select {
		case <-time.After(16 * time.Second):
		case <-req.Context().Done():
			return
		}
		json.NewEncoder(w).Encode(model.PlatformConsumerDNSRouteSourcesResponse{Assignment: a, Release: r, Snapshot: model.PlatformDNSRouteSourceSnapshot{SelectionDigest: "authenticated-selection"}})
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	got, err := (Client{BaseURL: server.URL}).DNSRouteSources(ctx, Identity{}, a, r)
	if err != nil || got.SelectionDigest != "authenticated-selection" {
		t.Fatal("complete source read was cut off at the interactive deadline", err)
	}
}

type timeoutSourceBody struct{}

func (timeoutSourceBody) Read([]byte) (int, error) { return 0, context.DeadlineExceeded }
func (timeoutSourceBody) Close() error             { return nil }

func TestSourceResponseTimeoutIsNotMalformedInput(t *testing.T) {
	for _, mode := range []string{"timeout", "malformed"} {
		t.Run(mode, func(t *testing.T) {
			client := Client{HTTPClient: &http.Client{Transport: sourceBudgetTransport(func(*http.Request) (*http.Response, error) {
				var body io.ReadCloser = timeoutSourceBody{}
				if mode == "malformed" {
					body = io.NopCloser(strings.NewReader("{invalid"))
				}
				return &http.Response{StatusCode: http.StatusOK, Body: body, Header: http.Header{}}, nil
			})}}
			err := client.json(t.Context(), "https://control.example/read", "scoped", http.MethodGet, nil, &struct{}{})
			if err == nil || errors.Is(err, ErrUnavailable) != (mode == "timeout") {
				t.Fatal("malformed and unavailable source reads were conflated", err)
			}
		})
	}
}
