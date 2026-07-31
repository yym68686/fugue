package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/localpvsafety"
	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
	"fugue/internal/store"

	"github.com/jackc/pgx/v5"
	"k8s.io/apimachinery/pkg/api/resource"
)

type fakeManagedPostgresPrimarySQLRow struct {
	inRecovery          bool
	transactionReadOnly string
	serverAddress       string
	err                 error
}

func (r fakeManagedPostgresPrimarySQLRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != 3 {
		return fmt.Errorf("unexpected readiness scan destination count %d", len(dest))
	}
	inRecovery, ok := dest[0].(*bool)
	if !ok {
		return fmt.Errorf("unexpected recovery scan destination %T", dest[0])
	}
	transactionReadOnly, ok := dest[1].(*string)
	if !ok {
		return fmt.Errorf("unexpected transaction mode scan destination %T", dest[1])
	}
	serverAddress, ok := dest[2].(*string)
	if !ok {
		return fmt.Errorf("unexpected server address scan destination %T", dest[2])
	}
	*inRecovery = r.inRecovery
	*transactionReadOnly = r.transactionReadOnly
	*serverAddress = r.serverAddress
	return nil
}

type fakeManagedPostgresPrimarySQLConnection struct {
	query  string
	row    pgx.Row
	closed bool
}

func (c *fakeManagedPostgresPrimarySQLConnection) QueryRow(_ context.Context, query string, _ ...any) pgx.Row {
	c.query = query
	return c.row
}

func (c *fakeManagedPostgresPrimarySQLConnection) Close(context.Context) error {
	c.closed = true
	return nil
}

func TestRecoverableManagedPostgresTransitionErrorPreservesCause(t *testing.T) {
	t.Parallel()

	cause := errors.New("connection reset after status patch")
	err := recoverableManagedPostgresTransitionError("switchover request", cause)
	if !errors.Is(err, cause) {
		t.Fatalf("expected recoverable transition error to preserve cause, got %v", err)
	}
	if !strings.Contains(err.Error(), "recoverable from observed cluster state") ||
		!strings.Contains(err.Error(), "without issuing a blind rollback") {
		t.Fatalf("expected actionable recovery semantics, got %v", err)
	}
}

func TestManagedPostgresReplicationLSNConverged(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		primary    string
		standby    string
		inRecovery bool
		want       bool
		wantErr    bool
	}{
		{name: "equal", primary: "0/16B6C50", standby: "0/16B6C50", inRecovery: true, want: true},
		{name: "standby ahead", primary: "0/16B6C50", standby: "0/16B6D00", inRecovery: true, want: true},
		{name: "standby behind", primary: "1/00000010", standby: "0/FFFFFFFF", inRecovery: true, want: false},
		{name: "promoted target", primary: "0/16B6C50", standby: "0/16B6C50", wantErr: true},
		{name: "missing replay lsn", primary: "0/16B6C50", standby: "", inRecovery: true, want: false},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := managedPostgresReplicationLSNConverged(test.primary, test.standby, test.inRecovery)
			if (err != nil) != test.wantErr {
				t.Fatalf("convergence error = %v, wantErr=%t", err, test.wantErr)
			}
			if got != test.want {
				t.Fatalf("convergence = %t, want %t", got, test.want)
			}
		})
	}
}

func TestManagedPostgresPodDatabaseURLHandlesIPv6AndEscapesCredentials(t *testing.T) {
	t.Parallel()

	raw := managedPostgresPodDatabaseURL("2001:db8::10", model.AppPostgresSpec{
		Database: "example db",
		User:     "example/user",
		Password: "secret:@/value",
	})
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse database URL: %v", err)
	}
	if parsed.Host != "[2001:db8::10]:5432" || parsed.Path != "/example db" {
		t.Fatalf("unexpected database URL target: host=%q path=%q", parsed.Host, parsed.Path)
	}
	if parsed.User.Username() != "example/user" {
		t.Fatalf("unexpected database URL user %q", parsed.User.Username())
	}
	password, ok := parsed.User.Password()
	if !ok || password != "secret:@/value" {
		t.Fatal("database URL password did not round-trip")
	}
	if parsed.Query().Get("sslmode") != "disable" || parsed.Query().Get("connect_timeout") != "5" {
		t.Fatalf("unexpected database URL query %q", parsed.RawQuery)
	}
}

func TestManagedPostgresPrimaryReadinessRequiresPodEndpointAndSQL(t *testing.T) {
	t.Parallel()

	const (
		namespace      = "tenant-demo"
		clusterName    = "demo-postgres"
		targetPrimary  = "demo-postgres-2"
		primaryIP      = "10.42.0.22"
		primaryNode    = "node-target"
		primaryRuntime = "runtime-target"
	)

	var podReady atomic.Bool
	var endpointMatches atomic.Bool
	var sqlReady atomic.Bool
	var sqlCalls atomic.Int32

	kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/apis/postgresql.cnpg.io/v1/namespaces/"+namespace+"/clusters/"+clusterName:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{"name": clusterName},
				"spec":     map[string]any{"instances": 2},
				"status": map[string]any{
					"readyInstances": 2,
					"currentPrimary": targetPrimary,
					"targetPrimary":  targetPrimary,
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/"+namespace+"/pods/"+targetPrimary:
			ready := "False"
			if podReady.Load() {
				ready = "True"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{"name": targetPrimary},
				"spec":     map[string]any{"nodeName": primaryNode},
				"status": map[string]any{
					"phase": "Running",
					"podIP": primaryIP,
					"conditions": []map[string]any{{
						"type":   "Ready",
						"status": ready,
					}},
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/nodes/"+primaryNode:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{
					"name":   primaryNode,
					"labels": map[string]any{runtimepkg.RuntimeIDLabelKey: primaryRuntime},
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/"+namespace+"/endpoints/"+clusterName+"-rw":
			endpointIP := "10.42.0.21"
			if endpointMatches.Load() {
				endpointIP = primaryIP
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"subsets": []map[string]any{{
					"addresses": []map[string]any{{"ip": endpointIP}},
				}},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer kubeServer.Close()

	client := &kubeClient{
		client:      kubeServer.Client(),
		baseURL:     kubeServer.URL,
		bearerToken: "test",
		namespace:   namespace,
	}
	postgres := model.AppPostgresSpec{
		Database: "demo",
		User:     "demo",
		Password: "secret",
	}
	svc := &Service{
		Config: config.ControllerConfig{
			ManagedAppRolloutTimeout: 25 * time.Millisecond,
		},
		Logger: log.New(io.Discard, "", 0),
		managedPostgresPrimarySQLProbe: func(_ context.Context, serviceHost, expectedPodIP string, got model.AppPostgresSpec) error {
			sqlCalls.Add(1)
			if serviceHost != clusterName+"-rw."+namespace+".svc" {
				t.Fatalf("unexpected SQL service host %q", serviceHost)
			}
			if expectedPodIP != primaryIP {
				t.Fatalf("unexpected SQL target Pod IP %q", expectedPodIP)
			}
			if got.Database != postgres.Database || got.User != postgres.User || got.Password != postgres.Password {
				t.Fatalf("unexpected SQL probe credentials: %+v", got)
			}
			if !sqlReady.Load() {
				return errors.New("database system is shutting down")
			}
			return nil
		},
	}

	placement, ready, detail, err := svc.observeManagedPostgresPrimaryReadiness(
		context.Background(), client, namespace, clusterName, targetPrimary, postgres,
	)
	if err != nil || ready || !strings.Contains(detail, "Ready condition") {
		t.Fatalf("unready primary pod must block completion: ready=%t detail=%q err=%v", ready, detail, err)
	}
	if got := sqlCalls.Load(); got != 0 {
		t.Fatalf("SQL probe ran before Pod readiness: calls=%d", got)
	}

	podReady.Store(true)
	placement, ready, detail, err = svc.observeManagedPostgresPrimaryReadiness(
		context.Background(), client, namespace, clusterName, targetPrimary, postgres,
	)
	if err != nil || ready || !strings.Contains(detail, "publish ready endpoint") {
		t.Fatalf("stale -rw endpoint must block completion: ready=%t detail=%q err=%v", ready, detail, err)
	}
	if got := sqlCalls.Load(); got != 0 {
		t.Fatalf("SQL probe ran before endpoint convergence: calls=%d", got)
	}

	endpointMatches.Store(true)
	placement, ready, detail, err = svc.observeManagedPostgresPrimaryReadiness(
		context.Background(), client, namespace, clusterName, targetPrimary, postgres,
	)
	if err != nil || ready || !strings.Contains(detail, "database system is shutting down") {
		t.Fatalf("transient SQL shutdown must remain pending: ready=%t detail=%q err=%v", ready, detail, err)
	}

	_, err = svc.waitForManagedPostgresPrimary(
		context.Background(), client, namespace, clusterName, targetPrimary, "", postgres,
	)
	if err == nil || !strings.Contains(err.Error(), "database system is shutting down") {
		t.Fatalf("wait gate completed while SQL was shutting down: %v", err)
	}

	sqlReady.Store(true)
	svc.Config.ManagedAppRolloutTimeout = time.Second
	placement, err = svc.waitForManagedPostgresPrimary(
		context.Background(), client, namespace, clusterName, targetPrimary, "", postgres,
	)
	if err != nil {
		t.Fatalf("ready primary must pass the completion gate: %v", err)
	}
	if placement.RuntimeID != primaryRuntime || placement.NodeName != primaryNode ||
		placement.PrimaryPod != targetPrimary || placement.PodIP != primaryIP {
		t.Fatalf("completion gate returned non-atomic placement witness: %+v", placement)
	}
	if got := sqlCalls.Load(); got < 3 {
		t.Fatalf("expected SQL readiness to be reprobed through convergence, calls=%d", got)
	}
}

func TestManagedPostgresEndpointSliceHonorsKubernetesReadinessSemantics(t *testing.T) {
	t.Parallel()

	ready := true
	notReady := false
	notServing := false
	terminating := true
	tests := []struct {
		name     string
		endpoint kubeEndpoint
		want     bool
	}{
		{
			name:     "nil conditions mean ready serving and non-terminating",
			endpoint: kubeEndpoint{Addresses: []string{"10.42.0.22"}},
			want:     true,
		},
		{
			name: "explicitly not ready",
			endpoint: func() kubeEndpoint {
				endpoint := kubeEndpoint{Addresses: []string{"10.42.0.22"}}
				endpoint.Conditions.Ready = &notReady
				return endpoint
			}(),
		},
		{
			name: "explicitly not serving",
			endpoint: func() kubeEndpoint {
				endpoint := kubeEndpoint{Addresses: []string{"10.42.0.22"}}
				endpoint.Conditions.Ready = &ready
				endpoint.Conditions.Serving = &notServing
				return endpoint
			}(),
		},
		{
			name: "terminating",
			endpoint: func() kubeEndpoint {
				endpoint := kubeEndpoint{Addresses: []string{"10.42.0.22"}}
				endpoint.Conditions.Ready = &ready
				endpoint.Conditions.Terminating = &terminating
				return endpoint
			}(),
		},
		{
			name: "explicitly ready",
			endpoint: func() kubeEndpoint {
				endpoint := kubeEndpoint{Addresses: []string{"10.42.0.22"}}
				endpoint.Conditions.Ready = &ready
				return endpoint
			}(),
			want: true,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := managedPostgresEndpointSlicesContainIP([]kubeEndpointSlice{{Endpoints: []kubeEndpoint{test.endpoint}}}, "10.42.0.22")
			if got != test.want {
				t.Fatalf("EndpointSlice readiness = %t, want %t", got, test.want)
			}
		})
	}
}

func TestManagedPostgresServiceDatabaseURLUsesReadinessApplicationName(t *testing.T) {
	t.Parallel()

	raw := managedPostgresServiceDatabaseURL("demo-postgres-rw.tenant-demo.svc", model.AppPostgresSpec{
		Database: "demo",
		User:     "demo",
		Password: "secret",
	})
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse database URL: %v", err)
	}
	if parsed.Host != "demo-postgres-rw.tenant-demo.svc:5432" {
		t.Fatalf("unexpected primary readiness host %q", parsed.Host)
	}
	if got := parsed.Query().Get("application_name"); got != "fugue-controller-primary-readiness" {
		t.Fatalf("unexpected readiness application_name %q", got)
	}
}

func TestProbeManagedPostgresPrimarySQLUsesDirectWritableSessionEvidence(t *testing.T) {
	t.Parallel()

	connection := &fakeManagedPostgresPrimarySQLConnection{
		row: fakeManagedPostgresPrimarySQLRow{
			serverAddress:       "10.42.0.22",
			transactionReadOnly: "off",
		},
	}
	var databaseURL string
	svc := &Service{
		postgresPrimarySQLConnect: func(_ context.Context, gotDatabaseURL string) (managedPostgresPrimarySQLConnection, error) {
			databaseURL = gotDatabaseURL
			return connection, nil
		},
	}
	err := svc.probeManagedPostgresPrimarySQL(
		context.Background(),
		"demo-postgres-rw.tenant-demo.svc",
		"10.42.0.22",
		model.AppPostgresSpec{Database: "demo", User: "demo", Password: "secret"},
	)
	if err != nil {
		t.Fatalf("writable direct pgx session must pass readiness: %v", err)
	}
	if !connection.closed {
		t.Fatal("expected readiness probe to close its pgx connection")
	}
	if strings.TrimSpace(connection.query) != strings.TrimSpace(managedPostgresPrimaryReadinessQuery) {
		t.Fatalf("unexpected readiness query %q", connection.query)
	}
	if !strings.Contains(connection.query, "host(inet_server_addr())") ||
		!strings.Contains(connection.query, "current_setting('transaction_read_only')") {
		t.Fatalf("readiness query must verify unmasked server identity and direct session write mode: %q", connection.query)
	}
	parsed, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatalf("parse readiness database URL: %v", err)
	}
	if parsed.Hostname() != "demo-postgres-rw.tenant-demo.svc" {
		t.Fatalf("readiness probe did not connect through the -rw service: %q", parsed.Host)
	}
}

func TestValidateManagedPostgresPrimarySQL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		serverAddress   string
		expectedPodIP   string
		transactionMode string
		inRecovery      bool
		wantError       string
	}{
		{name: "ready", serverAddress: "10.42.0.22", expectedPodIP: "10.42.0.22", transactionMode: "off"},
		{name: "postgres ipv4 cidr", serverAddress: "10.42.0.22/32", expectedPodIP: "10.42.0.22", transactionMode: "off"},
		{name: "canonical ipv6", serverAddress: "2001:db8::22", expectedPodIP: "2001:0db8:0:0:0:0:0:22", transactionMode: "OFF"},
		{name: "postgres ipv6 cidr", serverAddress: "2001:db8::22/128", expectedPodIP: "2001:0db8:0:0:0:0:0:22", transactionMode: "off"},
		{name: "wrong endpoint", serverAddress: "10.42.0.21", expectedPodIP: "10.42.0.22", transactionMode: "off", wantError: "instead of target primary"},
		{name: "invalid server address", serverAddress: "10.42.0.22/not-a-mask", expectedPodIP: "10.42.0.22", transactionMode: "off", wantError: "instead of target primary"},
		{name: "still standby", serverAddress: "10.42.0.22", expectedPodIP: "10.42.0.22", transactionMode: "off", inRecovery: true, wantError: "still in recovery"},
		{name: "read only", serverAddress: "10.42.0.22", expectedPodIP: "10.42.0.22", transactionMode: "on", wantError: "transaction_read_only=on"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := validateManagedPostgresPrimarySQL(test.serverAddress, test.expectedPodIP, test.transactionMode, test.inRecovery)
			if test.wantError == "" {
				if err != nil {
					t.Fatalf("ready SQL validation failed: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("SQL validation error = %v, want substring %q", err, test.wantError)
			}
		})
	}
}

func TestRollbackManagedPostgresStageRestoresThroughForcedApply(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Rollback Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "Rollback Project", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := stateStore.CreateApp(tenant.ID, project.ID, "rollback-app", "", model.AppSpec{
		Image:     "ghcr.io/example/rollback-app:latest",
		Replicas:  1,
		RuntimeID: model.DefaultManagedRuntimeID,
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	desired := app.Spec
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:    tenant.ID,
		Type:        model.OperationTypeDeploy,
		AppID:       app.ID,
		DesiredSpec: &desired,
	})
	if err != nil {
		t.Fatalf("create operation: %v", err)
	}
	claimed, found, err := stateStore.ClaimNextPendingOperation()
	if err != nil || !found || claimed.ID != op.ID {
		t.Fatalf("claim operation: found=%t operation=%+v err=%v", found, claimed, err)
	}

	svc := &Service{Store: stateStore, Logger: log.New(io.Discard, "", 0)}
	cause := errors.New("standby readiness timed out")
	rollbackCalled := false
	err = svc.rollbackManagedPostgresStage(context.Background(), claimed, cause, func(rollbackCtx context.Context) error {
		rollbackCalled = true
		if !forceExistingCloudNativePGWrites(rollbackCtx) {
			t.Fatal("expected rollback to force reconciliation of the previous CloudNativePG spec")
		}
		return nil
	})
	if !rollbackCalled {
		t.Fatal("expected staged-state rollback callback to run")
	}
	if !errors.Is(err, cause) || !strings.Contains(err.Error(), "was rolled back") {
		t.Fatalf("expected original failure plus successful rollback evidence, got %v", err)
	}
	stored, getErr := stateStore.GetOperation(op.ID)
	if getErr != nil {
		t.Fatalf("get operation: %v", getErr)
	}
	if stored.Status != model.OperationStatusRunning || !strings.Contains(stored.ResultMessage, "previous stable state restored") {
		t.Fatalf("expected running operation to record rollback phase, got %+v", stored)
	}
}

func TestDatabaseSwitchoverStagePreservesNodeAndFinalUsesWitness(t *testing.T) {
	t.Parallel()

	base := model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		RuntimeID: "runtime_app",
	}
	postgres := &model.AppPostgresSpec{
		Database:                         "demo",
		User:                             "demo",
		Password:                         "secret",
		RuntimeID:                        "runtime_source",
		FailoverTargetRuntimeID:          "runtime_target",
		PrimaryNodeName:                  "node-source",
		Instances:                        2,
		SynchronousReplicas:              1,
		PrimaryPlacementPendingRebalance: true,
	}

	stage := databaseSwitchoverStageSpec(base, postgres, "runtime_source", "runtime_target")
	if stage.Postgres == nil {
		t.Fatalf("expected postgres stage spec, got %+v", stage)
	}
	if stage.Postgres.PrimaryNodeName != "node-source" {
		t.Fatalf("stage changed durable source node pin: %+v", stage.Postgres)
	}
	final := databaseSwitchoverFinalPostgresSpec(postgres, "runtime_target", "runtime_source", "node-target")
	if final.PrimaryNodeName != "node-target" || final.RuntimeID != "runtime_target" ||
		final.FailoverTargetRuntimeID != "runtime_source" {
		t.Fatalf("final spec did not bind the target placement witness: %+v", final)
	}
	if final.PrimaryPlacementPendingRebalance {
		t.Fatalf("expected explicit switchover to clear pending placement hold, got %+v", final)
	}
}

func TestDatabaseLocalizeStageSpecPinsTargetNodeWithPlacementHold(t *testing.T) {
	t.Parallel()

	base := model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		RuntimeID: "runtime_app",
	}
	postgres := &model.AppPostgresSpec{
		Database:                         "demo",
		User:                             "demo",
		Password:                         "secret",
		RuntimeID:                        "runtime_source",
		FailoverTargetRuntimeID:          "runtime_target",
		PrimaryNodeName:                  "stale-target-node",
		Instances:                        2,
		SynchronousReplicas:              1,
		PrimaryPlacementPendingRebalance: true,
	}

	stage := databaseLocalizeStageSpec(base, postgres, "runtime_source", "runtime_app", "instance-us-1")
	if stage.Postgres == nil {
		t.Fatalf("expected postgres spec, got %+v", stage)
	}
	if stage.Postgres.RuntimeID != "runtime_app" || stage.Postgres.FailoverTargetRuntimeID != "" || stage.Postgres.PrimaryNodeName != "instance-us-1" {
		t.Fatalf("unexpected stage postgres placement: %+v", stage.Postgres)
	}
	if stage.Postgres.Instances != 2 || stage.Postgres.SynchronousReplicas != 0 || !stage.Postgres.PrimaryPlacementPendingRebalance {
		t.Fatalf("unexpected stage postgres lifecycle: %+v", stage.Postgres)
	}
}

func TestDatabaseLocalizeStageSpecHoldsCrossRuntimePrimaryWithoutTargetNode(t *testing.T) {
	t.Parallel()

	base := model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		RuntimeID: "runtime_app",
	}
	postgres := &model.AppPostgresSpec{
		Database:                "demo",
		User:                    "demo",
		Password:                "secret",
		RuntimeID:               "runtime_source",
		FailoverTargetRuntimeID: "runtime_old_target",
		Instances:               1,
	}

	stage := databaseLocalizeStageSpec(base, postgres, "runtime_source", "runtime_app", "")
	if stage.Postgres == nil {
		t.Fatalf("expected postgres spec, got %+v", stage)
	}
	if stage.Postgres.RuntimeID != "runtime_source" || stage.Postgres.FailoverTargetRuntimeID != "runtime_app" || stage.Postgres.PrimaryNodeName != "" {
		t.Fatalf("unexpected stage postgres placement: %+v", stage.Postgres)
	}
	if stage.Postgres.Instances != 2 || stage.Postgres.SynchronousReplicas != 1 || !stage.Postgres.PrimaryPlacementPendingRebalance {
		t.Fatalf("unexpected stage postgres lifecycle: %+v", stage.Postgres)
	}
}

func TestDatabaseLocalizeSpecClearsFailoverAndPinsTargetNode(t *testing.T) {
	t.Parallel()

	base := model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		RuntimeID: "runtime_app",
	}
	postgres := &model.AppPostgresSpec{
		Database:                         "demo",
		User:                             "demo",
		Password:                         "secret",
		RuntimeID:                        "runtime_source",
		FailoverTargetRuntimeID:          "runtime_target",
		PrimaryNodeName:                  "source-node",
		Instances:                        2,
		SynchronousReplicas:              1,
		PrimaryPlacementPendingRebalance: true,
	}

	final := databaseLocalizeSpec(base, postgres, "runtime_app", "instance-us-1", true, false)
	if final.Postgres == nil {
		t.Fatalf("expected postgres spec, got %+v", final)
	}
	if final.Postgres.RuntimeID != "runtime_app" || final.Postgres.FailoverTargetRuntimeID != "" || final.Postgres.PrimaryNodeName != "instance-us-1" {
		t.Fatalf("unexpected final postgres placement: %+v", final.Postgres)
	}
	if final.Postgres.Instances != 1 || final.Postgres.SynchronousReplicas != 0 || final.Postgres.PrimaryPlacementPendingRebalance {
		t.Fatalf("unexpected final postgres lifecycle: %+v", final.Postgres)
	}
}

func TestDatabaseLocalizeStorageMigrationForcesExtraReplica(t *testing.T) {
	t.Parallel()

	current := &model.AppPostgresSpec{
		StorageSize:         "1Gi",
		StorageClassName:    "fugue-local-rwo",
		Instances:           2,
		SynchronousReplicas: 1,
	}
	desired := &model.AppPostgresSpec{
		StorageSize:      "5Gi",
		StorageClassName: "fugue-postgres-rwo",
		Instances:        1,
	}
	merged := databaseLocalizeDesiredPostgresSpec(current, desired)
	if !managedPostgresStorageMigrationRequired(current, merged) {
		t.Fatal("expected storage migration to be required")
	}

	stage := databaseLocalizeStagePostgresSpec(merged, "runtime_app", "runtime_app", "")
	ensureDatabaseLocalizeStorageMigrationCapacity(&stage, current)
	if stage.StorageSize != "5Gi" || stage.StorageClassName != "fugue-postgres-rwo" {
		t.Fatalf("expected stage to use target storage, got %+v", stage)
	}
	if stage.Instances != 3 {
		t.Fatalf("expected storage migration to force one extra replica, got %+v", stage)
	}
}

func TestManagedPostgresInPlaceStorageExpansionRequired(t *testing.T) {
	t.Parallel()

	current := &model.AppPostgresSpec{
		StorageSize:      "5Gi",
		StorageClassName: "fugue-postgres-rwo",
	}
	desired := &model.AppPostgresSpec{
		StorageSize:      "10Gi",
		StorageClassName: "fugue-postgres-rwo",
	}

	if !managedPostgresInPlaceStorageExpansionRequired(current, desired, "runtime_a", "runtime_a", "") {
		t.Fatal("expected same-class storage growth to require in-place expansion")
	}

	desired.StorageClassName = "other"
	if managedPostgresInPlaceStorageExpansionRequired(current, desired, "runtime_a", "runtime_a", "") {
		t.Fatal("expected different storage class to fall back to migration path")
	}

	desired.StorageClassName = "fugue-postgres-rwo"
	if managedPostgresInPlaceStorageExpansionRequired(current, desired, "runtime_a", "runtime_b", "") {
		t.Fatal("expected cross-runtime storage growth to fall back to migration path")
	}
	if managedPostgresInPlaceStorageExpansionRequired(current, desired, "runtime_a", "runtime_a", "node-a") {
		t.Fatal("expected explicit target node to remain a placement localize")
	}
}

func TestPrepareManagedPostgresInPlaceStorageExpansionPatchesPVCRequest(t *testing.T) {
	t.Parallel()

	const namespace = "tenant-demo"
	const clusterName = "demo-postgres"
	const pvcName = "demo-postgres-1"

	var patchedStorage string
	kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/apis/storage.k8s.io/v1/storageclasses/fugue-postgres-rwo":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metadata":             map[string]any{"name": "fugue-postgres-rwo"},
				"allowVolumeExpansion": true,
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/"+namespace+"/persistentvolumeclaims":
			if got := r.URL.Query().Get("labelSelector"); got != "cnpg.io/cluster="+clusterName+",cnpg.io/pvcRole=PG_DATA" {
				t.Fatalf("unexpected pvc label selector %q", got)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{{
					"metadata": map[string]any{"name": pvcName},
				}},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/"+namespace+"/persistentvolumeclaims/"+pvcName:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{"name": pvcName},
				"spec": map[string]any{
					"storageClassName": "fugue-postgres-rwo",
					"resources": map[string]any{
						"requests": map[string]any{"storage": "5Gi"},
					},
				},
				"status": map[string]any{
					"capacity": map[string]any{"storage": "5Gi"},
				},
			})
		case r.Method == http.MethodPatch && r.URL.Path == "/api/v1/namespaces/"+namespace+"/persistentvolumeclaims/"+pvcName:
			var body struct {
				Spec struct {
					Resources struct {
						Requests map[string]string `json:"requests"`
					} `json:"resources"`
				} `json:"spec"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode pvc patch: %v", err)
			}
			patchedStorage = body.Spec.Resources.Requests["storage"]
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{"name": pvcName},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer kubeServer.Close()

	client := &kubeClient{
		client:      kubeServer.Client(),
		baseURL:     kubeServer.URL,
		bearerToken: "test",
		namespace:   namespace,
	}
	svc := &Service{
		Logger: log.New(io.Discard, "", 0),
	}

	err := svc.prepareManagedPostgresInPlaceStorageExpansion(context.Background(), client, namespace, clusterName, managedPostgresStorageTarget{
		StorageSize: "10Gi",
	})
	if err != nil {
		t.Fatalf("prepare in-place expansion: %v", err)
	}
	if patchedStorage != "10Gi" {
		t.Fatalf("expected pvc storage patch to 10Gi, got %q", patchedStorage)
	}
}

func TestPrepareManagedPostgresInPlaceStorageExpansionRequiresPVCForExistingCluster(t *testing.T) {
	t.Parallel()

	const namespace = "tenant-demo"
	const clusterName = "demo-postgres"

	kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/apis/storage.k8s.io/v1/storageclasses/fugue-postgres-rwo":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metadata":             map[string]any{"name": "fugue-postgres-rwo"},
				"allowVolumeExpansion": true,
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/"+namespace+"/persistentvolumeclaims":
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer kubeServer.Close()

	client := &kubeClient{
		client:      kubeServer.Client(),
		baseURL:     kubeServer.URL,
		bearerToken: "test",
		namespace:   namespace,
	}
	svc := &Service{Logger: log.New(io.Discard, "", 0)}
	target := managedPostgresStorageTarget{
		StorageClassName: "fugue-postgres-rwo",
		StorageSize:      "40Gi",
	}
	err := svc.prepareManagedPostgresInPlaceStorageExpansionForExistingCluster(
		context.Background(),
		client,
		namespace,
		clusterName,
		target,
	)
	if err == nil || !strings.Contains(err.Error(), "no postgres data PVCs found") {
		t.Fatalf("expected existing-cluster PVC preflight failure, got %v", err)
	}
	if err := svc.prepareManagedPostgresInPlaceStorageExpansion(
		context.Background(),
		client,
		namespace,
		clusterName,
		target,
	); err != nil {
		t.Fatalf("initial cluster without data PVCs must remain eligible for creation: %v", err)
	}
	if err := svc.prepareManagedPostgresInPlaceStorageExpansion(
		context.Background(),
		client,
		namespace,
		clusterName,
		managedPostgresStorageTarget{StorageSize: "40Gi"},
	); err != nil {
		t.Fatalf("initial cluster using the default storage class must remain eligible for creation: %v", err)
	}
}

func TestPrepareManagedPostgresInPlaceStorageExpansionRejectsInsufficientLocalPVCapacityBeforePatch(t *testing.T) {
	t.Parallel()

	const namespace = "tenant-demo"
	const clusterName = "demo-postgres"
	const pvcName = "demo-postgres-1"
	const nodeName = "worker-1"

	var patchCount int
	kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/apis/storage.k8s.io/v1/storageclasses/fugue-postgres-rwo":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metadata":             map[string]any{"name": "fugue-postgres-rwo"},
				"provisioner":          openEBSLocalLVMProvisioner,
				"parameters":           map[string]string{"volgroup": "fugue-vg"},
				"allowVolumeExpansion": true,
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/"+namespace+"/persistentvolumeclaims":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{{"metadata": map[string]any{"name": pvcName}}},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/"+namespace+"/persistentvolumeclaims/"+pvcName:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{"name": pvcName},
				"spec": map[string]any{
					"volumeName":       "pv-demo-postgres-1",
					"storageClassName": "fugue-postgres-rwo",
					"resources": map[string]any{
						"requests": map[string]any{"storage": "20Gi"},
					},
				},
				"status": map[string]any{
					"capacity": map[string]any{"storage": "20Gi"},
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/persistentvolumes/pv-demo-postgres-1":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{"name": "pv-demo-postgres-1"},
				"spec": map[string]any{
					"csi": map[string]any{
						"driver":           openEBSLocalLVMProvisioner,
						"volumeAttributes": map[string]string{"openebs.io/volgroup": "fugue-vg"},
					},
					"nodeAffinity": map[string]any{
						"required": map[string]any{
							"nodeSelectorTerms": []map[string]any{{
								"matchExpressions": []map[string]any{{
									"key":      "openebs.io/nodename",
									"operator": "In",
									"values":   []string{nodeName},
								}},
							}},
						},
					},
				},
			})
		case r.Method == http.MethodPatch && r.URL.Path == "/api/v1/namespaces/"+namespace+"/persistentvolumeclaims/"+pvcName:
			patchCount++
			_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"name": pvcName}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer kubeServer.Close()

	stateStore, _ := newImageCacheControllerTestStore(t)
	if _, err := stateStore.UpsertLocalPVInventory(model.LocalPVInventory{
		NodeID:          nodeName,
		ClusterNodeName: nodeName,
		VGName:          "fugue-vg",
		PVSizeBytes:     96 << 30,
		PVFreeBytes:     9 << 30,
		ObservedAt:      time.Now().UTC(),
	}); err != nil {
		t.Fatalf("upsert LocalPV inventory: %v", err)
	}
	client := &kubeClient{
		client:      kubeServer.Client(),
		baseURL:     kubeServer.URL,
		bearerToken: "test",
		namespace:   namespace,
	}
	svc := &Service{
		Store:  stateStore,
		Logger: log.New(io.Discard, "", 0),
	}
	err := svc.prepareManagedPostgresInPlaceStorageExpansion(
		context.Background(),
		client,
		namespace,
		clusterName,
		managedPostgresStorageTarget{
			StorageClassName: "fugue-postgres-rwo",
			StorageSize:      "40Gi",
		},
	)
	if err == nil || !strings.Contains(err.Error(), "insufficient LocalPV capacity") {
		t.Fatalf("expected LocalPV capacity rejection, got %v", err)
	}
	if patchCount != 0 {
		t.Fatalf("PVC request was patched before capacity preflight: patches=%d", patchCount)
	}
	if _, err := stateStore.UpsertLocalPVInventory(model.LocalPVInventory{
		NodeID:          nodeName,
		ClusterNodeName: nodeName,
		VGName:          "fugue-vg",
		PVSizeBytes:     96 << 30,
		PVFreeBytes:     32 << 30,
		ObservedAt:      time.Now().UTC().Add(-2 * localpvsafety.DefaultInventoryTTL),
	}); err != nil {
		t.Fatalf("upsert stale LocalPV inventory: %v", err)
	}
	err = svc.prepareManagedPostgresInPlaceStorageExpansion(
		context.Background(),
		client,
		namespace,
		clusterName,
		managedPostgresStorageTarget{
			StorageClassName: "fugue-postgres-rwo",
			StorageSize:      "40Gi",
		},
	)
	if err == nil || !strings.Contains(err.Error(), "is stale") {
		t.Fatalf("expected stale LocalPV inventory rejection, got %v", err)
	}
	if patchCount != 0 {
		t.Fatalf("PVC request was patched with stale inventory: patches=%d", patchCount)
	}
	if _, err := stateStore.UpsertLocalPVInventory(model.LocalPVInventory{
		NodeID:          nodeName,
		ClusterNodeName: nodeName,
		VGName:          "fugue-vg",
		PVSizeBytes:     96 << 30,
		PVFreeBytes:     32 << 30,
		ObservedAt:      time.Now().UTC(),
	}); err != nil {
		t.Fatalf("refresh LocalPV inventory: %v", err)
	}
	if err := svc.prepareManagedPostgresInPlaceStorageExpansion(
		context.Background(),
		client,
		namespace,
		clusterName,
		managedPostgresStorageTarget{
			StorageClassName: "fugue-postgres-rwo",
			StorageSize:      "40Gi",
		},
	); err != nil {
		t.Fatalf("expected expansion preflight to pass with sufficient capacity: %v", err)
	}
	if patchCount != 1 {
		t.Fatalf("expected one PVC patch after successful capacity preflight, got %d", patchCount)
	}
}

func TestInspectManagedPostgresStorageExpansionRequiresFilesystemCapacity(t *testing.T) {
	t.Parallel()

	const namespace = "tenant-demo"
	const clusterName = "demo-postgres"
	const pvcName = "demo-postgres-1"
	const podName = "demo-postgres-1"
	const replacementPodName = "demo-postgres-1-replacement"
	const nodeName = "worker-1"

	filesystemCapacity := uint64(28 << 30)
	replacementFilesystemCapacity := uint64(40 << 30)
	kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/"+namespace+"/persistentvolumeclaims":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{{"metadata": map[string]any{"name": pvcName}}},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/"+namespace+"/persistentvolumeclaims/"+pvcName:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{"name": pvcName},
				"spec": map[string]any{
					"resources": map[string]any{"requests": map[string]any{"storage": "40Gi"}},
				},
				"status": map[string]any{
					"capacity":           map[string]any{"storage": "40Gi"},
					"allocatedResources": map[string]any{"storage": "40Gi"},
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/"+namespace+"/pods":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{
					{
						"metadata": map[string]any{"name": podName},
						"spec": map[string]any{
							"nodeName": nodeName,
							"volumes": []map[string]any{{
								"name":                  "pgdata",
								"persistentVolumeClaim": map[string]any{"claimName": pvcName},
							}},
						},
					},
					{
						"metadata": map[string]any{"name": replacementPodName},
						"spec": map[string]any{
							"nodeName": nodeName,
							"volumes": []map[string]any{{
								"name":                  "pgdata",
								"persistentVolumeClaim": map[string]any{"claimName": pvcName},
							}},
						},
					},
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/nodes/"+nodeName+"/proxy/stats/summary":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"pods": []map[string]any{
					{
						"podRef": map[string]any{"name": podName, "namespace": namespace},
						"volume": []map[string]any{{
							"pvcRef":        map[string]any{"name": pvcName, "namespace": namespace},
							"capacityBytes": filesystemCapacity,
						}},
					},
					{
						"podRef": map[string]any{"name": replacementPodName, "namespace": namespace},
						"volume": []map[string]any{{
							"pvcRef":        map[string]any{"name": pvcName, "namespace": namespace},
							"capacityBytes": replacementFilesystemCapacity,
						}},
					},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer kubeServer.Close()

	client := &kubeClient{
		client:      kubeServer.Client(),
		baseURL:     kubeServer.URL,
		bearerToken: "test",
		namespace:   namespace,
	}
	target := managedPostgresStorageTarget{
		StorageClassName: "fugue-postgres-rwo",
		StorageSize:      "40Gi",
	}
	converged, detail, err := inspectManagedPostgresStorageExpansion(
		context.Background(),
		client,
		namespace,
		clusterName,
		target,
	)
	if err != nil {
		t.Fatalf("inspect expansion: %v", err)
	}
	if converged || !strings.Contains(detail, "filesystem capacity") {
		t.Fatalf("expected filesystem convergence wait, converged=%t detail=%q", converged, detail)
	}

	filesystemCapacity = 40 << 30
	converged, detail, err = inspectManagedPostgresStorageExpansion(
		context.Background(),
		client,
		namespace,
		clusterName,
		target,
	)
	if err != nil {
		t.Fatalf("inspect converged expansion: %v", err)
	}
	if !converged || detail != "" {
		t.Fatalf("expected converged expansion, converged=%t detail=%q", converged, detail)
	}
}

func TestManagedDeployStorageConvergenceRejectsPVCResizeError(t *testing.T) {
	t.Parallel()

	const namespace = "tenant-demo"
	const clusterName = "demo-postgres"
	const pvcName = "demo-postgres-1"

	kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/apis/postgresql.cnpg.io/v1/namespaces/"+namespace+"/clusters/"+clusterName:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{"name": clusterName},
				"spec":     map[string]any{"instances": 1},
				"status": map[string]any{
					"readyInstances": 1,
					"currentPrimary": clusterName + "-1",
					"targetPrimary":  clusterName + "-1",
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/"+namespace+"/persistentvolumeclaims":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{{"metadata": map[string]any{"name": pvcName}}},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/"+namespace+"/pods":
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/"+namespace+"/persistentvolumeclaims/"+pvcName:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{"name": pvcName},
				"spec": map[string]any{
					"resources": map[string]any{"requests": map[string]any{"storage": "40Gi"}},
				},
				"status": map[string]any{
					"capacity":                  map[string]any{"storage": "20Gi"},
					"allocatedResources":        map[string]any{"storage": "40Gi"},
					"allocatedResourceStatuses": map[string]any{"storage": "NodeResizeError"},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer kubeServer.Close()

	client := &kubeClient{
		client:      kubeServer.Client(),
		baseURL:     kubeServer.URL,
		bearerToken: "test",
		namespace:   namespace,
	}
	svc := &Service{
		Config: config.ControllerConfig{
			ManagedAppRolloutTimeout: time.Second,
			PollInterval:             time.Millisecond,
		},
		Logger: log.New(io.Discard, "", 0),
		newKubeClient: func(string) (*kubeClient, error) {
			return client, nil
		},
	}
	err := svc.waitForManagedPostgresStorageConvergenceForDeployments(
		context.Background(),
		"",
		namespace,
		[]runtimepkg.ManagedBackingServiceDeployment{{
			ResourceName:     clusterName,
			ResourceKind:     runtimepkg.CloudNativePGClusterKind,
			StorageClassName: "fugue-postgres-rwo",
			StorageSize:      "40Gi",
		}},
	)
	if err == nil || !strings.Contains(err.Error(), "NodeResizeError") {
		t.Fatalf("managed deploy must reject a PVC resize error before operation completion, got %v", err)
	}

	err = svc.waitForManagedPostgresStorageConvergenceForDeployments(
		context.Background(),
		"",
		namespace,
		[]runtimepkg.ManagedBackingServiceDeployment{{
			ResourceName:     clusterName,
			ResourceKind:     runtimepkg.CloudNativePGClusterKind,
			Suspended:        true,
			StorageClassName: "fugue-postgres-rwo",
			StorageSize:      "40Gi",
		}},
	)
	if err != nil {
		t.Fatalf("intentionally suspended postgres must defer mounted filesystem verification: %v", err)
	}
}

func TestLocalPVExpansionCapacityDoesNotReserveAlreadyRequestedGrowthTwice(t *testing.T) {
	t.Parallel()

	target, err := resource.ParseQuantity("40Gi")
	if err != nil {
		t.Fatalf("parse target: %v", err)
	}
	err = (&Service{}).validateManagedPostgresLocalPVExpansionCapacity(
		context.Background(),
		nil,
		"tenant-demo",
		kubeStorageClass{},
		target,
		[]managedPostgresPVCExpansionPlan{{
			Name:             "demo-postgres-1",
			RequestConverged: true,
		}},
	)
	if err != nil {
		t.Fatalf("already-requested expansion must proceed to convergence checks without reserving capacity twice: %v", err)
	}
}

func TestManagedPostgresPVCResizeErrorDetectsNodeResizeError(t *testing.T) {
	t.Parallel()

	pvc := kubePersistentVolumeClaim{}
	pvc.Status.AllocatedResourceStatuses = map[string]string{"storage": "NodeResizeError"}
	if got := managedPostgresPVCResizeError(pvc); !strings.Contains(got, "NodeResizeError") {
		t.Fatalf("resize error = %q", got)
	}
}

func TestPersistentVolumeNodeNameRejectsAmbiguousAffinity(t *testing.T) {
	t.Parallel()

	var pv kubePersistentVolume
	if err := json.Unmarshal([]byte(`{
		"spec": {
			"nodeAffinity": {
				"required": {
					"nodeSelectorTerms": [{
						"matchExpressions": [{
							"key": "openebs.io/nodename",
							"operator": "In",
							"values": ["worker-1", "worker-2"]
						}]
					}]
				}
			}
		}
	}`), &pv); err != nil {
		t.Fatalf("decode PV fixture: %v", err)
	}
	if got := persistentVolumeNodeName(pv); got != "" {
		t.Fatalf("ambiguous node affinity resolved to %q", got)
	}
}

func TestPrepareManagedPostgresStorageMigrationExpansionTemporarilyEnablesLegacyClass(t *testing.T) {
	t.Parallel()

	var patchValues []bool
	kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/tenant-demo/persistentvolumeclaims":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{{
					"metadata": map[string]any{"name": "demo-postgres-1"},
				}},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/tenant-demo/persistentvolumeclaims/demo-postgres-1":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{"name": "demo-postgres-1"},
				"spec": map[string]any{
					"storageClassName": "local-path",
					"resources": map[string]any{
						"requests": map[string]any{"storage": "1Gi"},
					},
				},
				"status": map[string]any{
					"capacity": map[string]any{"storage": "1Gi"},
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/apis/storage.k8s.io/v1/storageclasses/local-path":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metadata":             map[string]any{"name": "local-path"},
				"allowVolumeExpansion": false,
			})
		case r.Method == http.MethodPatch && r.URL.Path == "/apis/storage.k8s.io/v1/storageclasses/local-path":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode storage class patch: %v", err)
			}
			value, ok := body["allowVolumeExpansion"].(bool)
			if !ok {
				t.Fatalf("expected allowVolumeExpansion patch, got %+v", body)
			}
			patchValues = append(patchValues, value)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metadata":             map[string]any{"name": "local-path"},
				"allowVolumeExpansion": value,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer kubeServer.Close()

	client := &kubeClient{
		client:      kubeServer.Client(),
		baseURL:     kubeServer.URL,
		bearerToken: "test",
		namespace:   "tenant-demo",
	}
	svc := &Service{
		Logger: log.New(io.Discard, "", 0),
	}

	restore, err := svc.prepareManagedPostgresStorageMigrationExpansion(context.Background(), client, "tenant-demo", "demo-postgres", managedPostgresStorageTarget{
		StorageClassName: "fugue-postgres-rwo",
		StorageSize:      "5Gi",
	})
	if err != nil {
		t.Fatalf("prepare storage migration expansion: %v", err)
	}
	if restore == nil {
		t.Fatal("expected restore function")
	}
	if len(patchValues) != 1 || !patchValues[0] {
		t.Fatalf("expected temporary allowVolumeExpansion=true patch, got %v", patchValues)
	}
	if err := restore(context.Background()); err != nil {
		t.Fatalf("restore storage class expansion: %v", err)
	}
	if len(patchValues) != 2 || patchValues[1] {
		t.Fatalf("expected restore allowVolumeExpansion=false patch, got %v", patchValues)
	}
}

func TestBindManagedPostgresPendingReplicaOnNodeBindsPVCOnTargetNode(t *testing.T) {
	t.Parallel()

	var boundNode string
	kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/tenant-demo/pods":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{
					{
						"metadata": map[string]any{"name": "demo-postgres-1"},
						"status":   map[string]any{"phase": "Running"},
					},
					{
						"metadata": map[string]any{"name": "demo-postgres-3"},
						"spec": map[string]any{
							"volumes": []map[string]any{{
								"name": "pgdata",
								"persistentVolumeClaim": map[string]any{
									"claimName": "demo-postgres-3",
								},
							}},
						},
						"status": map[string]any{"phase": "Pending"},
					},
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/tenant-demo/persistentvolumeclaims/demo-postgres-3":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{"name": "demo-postgres-3"},
				"spec": map[string]any{
					"storageClassName": "fugue-postgres-rwo",
					"volumeName":       "pv-demo-postgres-3",
					"resources": map[string]any{
						"requests": map[string]any{"storage": "5Gi"},
					},
				},
				"status": map[string]any{
					"capacity": map[string]any{"storage": "5Gi"},
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/persistentvolumes/pv-demo-postgres-3":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{"name": "pv-demo-postgres-3"},
				"spec": map[string]any{
					"nodeAffinity": map[string]any{
						"required": map[string]any{
							"nodeSelectorTerms": []map[string]any{{
								"matchExpressions": []map[string]any{{
									"key":      "openebs.io/nodename",
									"operator": "In",
									"values":   []string{"node-a"},
								}},
							}},
						},
					},
				},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/namespaces/tenant-demo/pods/demo-postgres-3/binding":
			var body struct {
				Target struct {
					Name string `json:"name"`
				} `json:"target"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode binding body: %v", err)
			}
			boundNode = body.Target.Name
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"kind": "Binding"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer kubeServer.Close()

	client := &kubeClient{
		client:      kubeServer.Client(),
		baseURL:     kubeServer.URL,
		bearerToken: "test",
		namespace:   "tenant-demo",
	}
	svc := &Service{
		Logger: log.New(io.Discard, "", 0),
	}

	podName, err := svc.bindManagedPostgresPendingReplicaOnNode(context.Background(), client, "tenant-demo", "demo-postgres", "node-a", "demo-postgres-1", managedPostgresStorageTarget{
		StorageClassName: "fugue-postgres-rwo",
		StorageSize:      "5Gi",
	})
	if err != nil {
		t.Fatalf("bind pending replica: %v", err)
	}
	if podName != "demo-postgres-3" || boundNode != "node-a" {
		t.Fatalf("expected demo-postgres-3 bound to node-a, got pod=%q node=%q", podName, boundNode)
	}
}

func TestSelectManagedPostgresSwitchoverTargetMatchesManagedSharedLocationNode(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	if err := stateStore.SyncManagedSharedLocationRuntimes([]map[string]string{{
		runtimepkg.LocationCountryCodeLabelKey: "us",
	}}); err != nil {
		t.Fatalf("sync shared location runtimes: %v", err)
	}

	targetRuntimeID := managedSharedRuntimeIDForLabels(t, stateStore, map[string]string{
		runtimepkg.LocationCountryCodeLabelKey: "us",
	})
	namespace := "tenant-demo"
	clusterName := "demo-postgres"
	currentPrimary := "demo-postgres-1"
	standbyPod := "demo-postgres-2"
	standbyNode := "instance-us-1"

	kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/api/v1/namespaces/" + namespace + "/pods":
			if err := json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{
					{
						"metadata": map[string]any{
							"name":              currentPrimary,
							"creationTimestamp": "2026-04-07T00:00:00Z",
						},
						"spec": map[string]any{
							"nodeName": "instance-hk-1",
						},
						"status": map[string]any{
							"phase": "Running",
						},
					},
					{
						"metadata": map[string]any{
							"name":              standbyPod,
							"creationTimestamp": "2026-04-07T00:01:00Z",
						},
						"spec": map[string]any{
							"nodeName": standbyNode,
						},
						"status": map[string]any{
							"phase": "Running",
						},
					},
				},
			}); err != nil {
				t.Fatalf("encode pod list: %v", err)
			}
		case "/api/v1/nodes/" + standbyNode:
			if err := json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{
					"name": standbyNode,
					"labels": map[string]any{
						runtimepkg.SharedPoolLabelKey:          runtimepkg.SharedPoolLabelValue,
						runtimepkg.LocationCountryCodeLabelKey: "us",
						kubeHostnameLabelKey:                   standbyNode,
					},
				},
			}); err != nil {
				t.Fatalf("encode standby node: %v", err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer kubeServer.Close()

	client := &kubeClient{
		client:      kubeServer.Client(),
		baseURL:     kubeServer.URL,
		bearerToken: "test",
		namespace:   namespace,
	}
	svc := &Service{
		Store:  stateStore,
		Logger: log.New(io.Discard, "", 0),
	}

	got, err := svc.selectManagedPostgresSwitchoverTarget(context.Background(), client, namespace, clusterName, targetRuntimeID, currentPrimary, managedPostgresStorageTarget{})
	if err != nil {
		t.Fatalf("select switchover target: %v", err)
	}
	if got != standbyPod {
		t.Fatalf("expected standby pod %q, got %q", standbyPod, got)
	}
}
