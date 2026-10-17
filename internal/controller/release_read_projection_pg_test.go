package controller

import (
	"context"
	"database/sql"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/routeproof"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

type postgresResponseCounter struct {
	io.Writer
	bytes *atomic.Int64
}

func (w postgresResponseCounter) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	w.bytes.Add(int64(n))
	return n, err
}

// Count bytes actually returned over the disposable PostgreSQL connection,
// independently of which store method or SQL projection the caller chooses.
func controllerProjectionPostgres(t *testing.T) (*store.Store, *sql.DB, *atomic.Int64) {
	t.Helper()
	address := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if address == "" {
		t.Skip("requires disposable loopback PostgreSQL")
	}
	u, err := url.Parse(address)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("requires a disposable loopback fugue_test database")
	}
	db, err := sql.Open("pgx", address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var received atomic.Int64
	var connections sync.Map
	var workers sync.WaitGroup
	stopped, accepted := make(chan struct{}), make(chan struct{})
	target := u.Host
	go func() {
		defer close(accepted)
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			connections.Store(client, true)
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer client.Close()
				defer connections.Delete(client)
				server, err := net.DialTimeout("tcp", target, 2*time.Second)
				if err != nil {
					return
				}
				defer server.Close()
				connections.Store(server, true)
				defer connections.Delete(server)
				select {
				case <-stopped:
					return
				default:
				}
				sent := make(chan struct{})
				go func() { _, _ = io.Copy(server, client); _ = server.Close(); close(sent) }()
				_, _ = io.Copy(postgresResponseCounter{Writer: client, bytes: &received}, server)
				_ = client.Close()
				<-sent
			}()
		}
	}()
	t.Cleanup(func() {
		close(stopped)
		_ = listener.Close()
		<-accepted
		connections.Range(func(key, _ any) bool { _ = key.(net.Conn).Close(); return true })
		workers.Wait()
	})
	u.Host = listener.Addr().String()
	state := store.New(filepath.Join(t.TempDir(), "state.json"), u.String())
	if err := state.Init(); err != nil {
		t.Fatal(err)
	}
	return state, db, &received
}

func TestControllerReleaseMetadataPathsBoundPostgresTransfer(t *testing.T) {
	state, db, received := controllerProjectionPostgres(t)
	tenant, err := state.CreateTenant(model.NewID("release_projection"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := db.Exec(`DELETE FROM fugue_tenants WHERE id=$1`, tenant.ID); err != nil {
			t.Error(err)
		}
	})
	project, err := state.CreateProject(tenant.ID, "projection", "")
	if err != nil {
		t.Fatal(err)
	}
	app, err := state.CreateApp(tenant.ID, project.ID, "projection", "", model.AppSpec{Image: "registry.example/app:v1", RuntimeID: model.DefaultManagedRuntimeID, Replicas: 1, Ports: []int{8080}})
	if err != nil {
		t.Fatal(err)
	}
	app.Route = &model.AppRoute{Hostname: "projection.example.test", PathPrefix: "/", ServicePort: 8080}
	large := strings.Repeat("configuration", 100000)
	var releases []model.AppRelease
	for _, kind := range []struct{ role, status string }{
		{model.AppReleaseRoleStable, model.AppReleaseStatusServing},
		{model.AppReleaseRoleCandidate, model.AppReleaseStatusReady},
		{model.AppReleaseRolePrevious, model.AppReleaseStatusDraining},
		{model.AppReleaseRoleCandidate, model.AppReleaseStatusFailed},
		{model.AppReleaseRoleCandidate, model.AppReleaseStatusCreating},
		{model.AppReleaseRoleRetired, model.AppReleaseStatusRetired},
	} {
		release, err := state.CreateAppRelease(model.AppRelease{
			TenantID: tenant.ID, AppID: app.ID, Role: kind.role, Status: kind.status,
			RuntimeID: app.Spec.RuntimeID, ResolvedImageRef: app.Spec.Image,
			DeploymentName: model.NewID("deployment"), ServiceName: model.NewID("service"),
			UpstreamURL:  "http://revision.example.test:8080",
			SpecSnapshot: &model.AppSpec{Image: app.Spec.Image, Env: map[string]string{"LARGE_CONFIG": large}},
		})
		if err != nil {
			t.Fatal(err)
		}
		releases = append(releases, release)
	}
	stable, candidate := releases[0], releases[1]
	if _, err = state.UpsertAppTrafficPolicy(model.AppTrafficPolicy{TenantID: tenant.ID, AppID: app.ID, Mode: model.AppTrafficModeCanary, StableReleaseID: stable.ID, CandidateReleaseID: candidate.ID, StableWeight: 80, CandidateWeight: 20}); err != nil {
		t.Fatal(err)
	}
	measure := func(t *testing.T, run func()) {
		t.Helper()
		before := received.Load()
		run()
		bytes := received.Load() - before
		t.Logf("PostgreSQL response bytes: %d", bytes)
		if bytes > 64<<10 {
			t.Errorf("identity-only observation transferred executable snapshots: %d bytes", bytes)
		}
	}
	t.Run("resource protection", func(t *testing.T) {
		measure(t, func() {
			protected := map[string]map[string]struct{}{}
			if err := (&Service{Store: state}).preserveActiveAppReleaseResources(app, protected); err != nil {
				t.Fatal(err)
			}
			for _, release := range releases {
				options := runtime.RenderOptions{Revision: safeRolloutCandidateRevision(release.ID)}
				for kind, names := range map[string][]string{"Deployment": {release.DeploymentName, runtime.RuntimeAppResourceNameWithOptions(app, options)}, "Service": {release.ServiceName, runtime.RuntimeAppServiceNameWithOptions(app, options)}} {
					for _, name := range names {
						_, found := protected[kind][name]
						if found != (release.Status != model.AppReleaseStatusRetired) {
							t.Fatalf("resource protection changed for %s %s in %s", kind, name, release.Status)
						}
					}
				}
			}
		})
	})
	t.Run("application traffic proof", func(t *testing.T) {
		measure(t, func() {
			observer := storeSafeRolloutEdgeBundleObserver{Traffic: state}
			got, err := observer.expectedAppTrafficDigest(app, candidate, 20)
			if err != nil {
				t.Fatal(err)
			}
			upstream := func(release model.AppRelease, weight int) model.EdgeRouteUpstream {
				return model.EdgeRouteUpstream{Role: release.Role, ReleaseID: release.ID, Weight: weight, UpstreamKind: model.EdgeRouteUpstreamKindKubernetesService, UpstreamScope: model.EdgeRouteUpstreamScopeLocalService, UpstreamURL: release.UpstreamURL, ServicePort: 8080, RuntimeID: release.RuntimeID, DeploymentGeneration: release.ResolvedImageRef}
			}
			want, err := routeproof.AppTrafficDigest(model.EdgeRouteBinding{Hostname: app.Route.Hostname, PathPrefix: "/", AppID: app.ID, TenantID: app.TenantID, Upstreams: []model.EdgeRouteUpstream{upstream(stable, 80), upstream(candidate, 20)}})
			if err != nil || got != want {
				t.Fatal("metadata changed exact traffic proof", got, want, err)
			}
		})
	})
	t.Run("retirement reads only drain configurations", func(t *testing.T) {
		before := received.Load()
		got, err := state.ListAppReleaseRetirementState(context.Background(), model.AppReleaseFilter{TenantID: tenant.ID, AppID: app.ID, IncludeRetired: true})
		bytes := received.Load() - before
		if err != nil || len(got) != len(releases) {
			t.Fatal("incomplete retirement state", len(got), err)
		}
		configs := 0
		for _, release := range got {
			if store.AppReleaseAwaitingDrain(release) {
				if release.SpecSnapshot == nil || release.SpecSnapshot.Env["LARGE_CONFIG"] != large {
					t.Fatal("lost executable drain configuration")
				}
				configs++
			} else if release.SpecSnapshot != nil {
				t.Fatal("unneeded snapshot transferred")
			}
		}
		t.Logf("PostgreSQL response bytes for %d required snapshots: %d", configs, bytes)
		if configs != 2 || bytes < int64(2*len(large)) || bytes > int64(2*len(large)+(64<<10)) {
			t.Fatal("unexpected retirement transfer", configs, bytes)
		}
	})
	for _, release := range releases {
		stored, err := state.GetAppRelease(tenant.ID, false, release.ID)
		if err != nil || stored.SpecSnapshot == nil || stored.SpecSnapshot.Env["LARGE_CONFIG"] != large {
			t.Fatal("observation changed retained executable configuration", err)
		}
	}
}
