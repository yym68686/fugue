package releaseflow

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"fugue/internal/model"
)

func BenchmarkReleaseGateEvaluator(b *testing.B) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	b.Cleanup(upstream.Close)

	evaluator := ReleaseGateEvaluator{
		MetricsQuerier: staticReleaseGateMetricsQuerier{metrics: map[string]any{
			"request_count":             100,
			"error_5xx_rate":            0.001,
			"edge_upstream_error_rate":  0.001,
			"p95_ttfb_ms":               80,
			"p99_duration_ms":           250,
			"edge_upstream_error_count": 0,
		}},
	}
	app := model.App{ID: "app_bench", TenantID: "tenant_bench"}
	release := model.AppRelease{
		ID:          "rel_bench",
		AppID:       app.ID,
		Role:        model.AppReleaseRoleCandidate,
		UpstreamURL: upstream.URL,
		Status:      model.AppReleaseStatusReady,
	}
	policy := evaluator.NormalizePolicy(&model.AppReleaseGatePolicy{
		WindowSeconds:        60,
		MinCandidateRequests: 10,
		Max5xxRate:           0.01,
	})

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		result := evaluator.Evaluate(context.Background(), app, release, policy)
		if result.Status != model.AppReleaseGateStatusPass {
			b.Fatalf("expected pass, got %+v", result)
		}
	}
}

type staticReleaseGateMetricsQuerier struct {
	metrics map[string]any
}

func (q staticReleaseGateMetricsQuerier) QueryReleaseGateMetrics(context.Context, string, string, string, time.Duration) (map[string]any, error) {
	return q.metrics, nil
}
