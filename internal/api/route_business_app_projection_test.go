package api

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestRouteBusinessAppProjectionPreservesRouteAndRequestBodyPolicy(t *testing.T) {
	for _, tc := range []struct {
		name, policy string
		active       bool
	}{
		{"valid", `[{"name":"upload","methods":["POST"],"paths":["/upload"],"max_bytes":1048576,"timeout_seconds":30,"max_concurrent":2,"retry_after_seconds":5}]`, true},
		{"invalid", `[{"name":"unsafe"}]`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, server, _, _, fixture, _ := setupAppDomainTestServerWithDomains(t, "example.test")
			spec := fixture.Spec
			spec.Env = map[string]string{model.AppEdgeRequestBodyPoliciesEnv: tc.policy}
			spec.Files = []model.AppFile{{Path: "/config", Content: strings.Repeat("private", 200000), Secret: true}}
			spec.Command, spec.Args = []string{"run"}, []string{"--serve"}
			spec.GeneratedEnv = map[string]model.AppGeneratedEnvSpec{"GENERATED": {Generate: "random", Length: 32}}
			app, err := s.CreateAppWithRoute(fixture.TenantID, fixture.ProjectID, "projection", "", spec, model.AppRoute{Hostname: "projection.example.test", ServicePort: 8080})
			if err != nil {
				t.Fatal(err)
			}
			full, err := s.GetApp(app.ID)
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := s.CaptureRouteBusinessSnapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			var projected model.App
			for _, candidate := range snapshot.Apps {
				if candidate.ID == app.ID {
					projected = candidate
				}
			}
			if projected.ID == "" || len(projected.Spec.Files) != 0 {
				t.Fatal("missing or unprojected compiler app")
			}
			full.Status.CurrentReplicas, projected.Status.CurrentReplicas = 1, 1
			now := time.Now().UTC()
			runtimes := map[string]model.Runtime{app.Spec.RuntimeID: {ID: app.Spec.RuntimeID, Type: model.RuntimeTypeManagedShared}}
			compile := func(app model.App) model.EdgeRouteBinding {
				return server.compileTrafficEpochRouteBinding(context.Background(), app, app.Route.Hostname, model.EdgeRouteKindPlatform, model.EdgeRouteTLSPolicyPlatform, now, now, runtimes, nil)
			}
			want, got := compile(full), compile(projected)
			if !reflect.DeepEqual(got, want) {
				t.Fatal("compiler projection changed route material")
			}
			if tc.active {
				if got.Status != model.EdgeRouteStatusActive || len(got.RequestBodyPolicies) != 1 || got.RequestBodyPolicies[0].MaxBytes != 1048576 {
					t.Fatal("valid request-body policy lost")
				}
			} else if got.Status != model.EdgeRouteStatusUnavailable || got.UpstreamURL != "" || got.StatusReason != "invalid app edge request body policy" {
				t.Fatal("invalid request-body policy no longer fails closed")
			}
		})
	}
}
