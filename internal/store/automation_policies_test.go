package store

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestAutomationPolicyJSONStoreLifecycleAndIsolation(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "store.json")
	stateStore := New(storePath)

	tenantA, err := stateStore.CreateTenant("Automation Tenant A")
	if err != nil {
		t.Fatal(err)
	}
	projectA, err := stateStore.CreateProject(tenantA.ID, "Project A", "")
	if err != nil {
		t.Fatal(err)
	}
	projectA2, err := stateStore.CreateProject(tenantA.ID, "Project A2", "")
	if err != nil {
		t.Fatal(err)
	}
	tenantB, err := stateStore.CreateTenant("Automation Tenant B")
	if err != nil {
		t.Fatal(err)
	}
	projectB, err := stateStore.CreateProject(tenantB.ID, "Project B", "")
	if err != nil {
		t.Fatal(err)
	}

	input := testAutomationPolicy(tenantA.ID, projectA.ID, "API Recovery")
	input.Generation = 99
	input.CreatedAt = time.Unix(1, 0).UTC()
	input.UpdatedAt = input.CreatedAt
	created, err := stateStore.CreateAutomationPolicy(input)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(created.ID, "automation_policy_") {
		t.Fatalf("unexpected generated ID %q", created.ID)
	}
	if created.Generation != 1 {
		t.Fatalf("generation=%d, want 1", created.Generation)
	}
	if created.CreatedAt.IsZero() || !created.CreatedAt.Equal(created.UpdatedAt) || created.CreatedAt.Equal(input.CreatedAt) {
		t.Fatalf("store did not own initial timestamps: created=%s updated=%s", created.CreatedAt, created.UpdatedAt)
	}
	if created.Rules[0].Action.Parameters["reason"] != "restart unhealthy app" {
		t.Fatalf("parameters were not normalized: %+v", created.Rules[0].Action.Parameters)
	}

	if _, err := stateStore.CreateAutomationPolicy(testAutomationPolicy(tenantA.ID, projectA.ID, " api recovery ")); !errors.Is(err, ErrConflict) {
		t.Fatalf("case-insensitive duplicate name error=%v, want conflict", err)
	}
	if _, err := stateStore.CreateAutomationPolicy(testAutomationPolicy(tenantA.ID, projectA2.ID, "API Recovery")); err != nil {
		t.Fatalf("same name in another project should be valid: %v", err)
	}
	tenantBPolicy, err := stateStore.CreateAutomationPolicy(testAutomationPolicy(tenantB.ID, projectB.ID, "API Recovery"))
	if err != nil {
		t.Fatal(err)
	}

	tenantPolicies, err := stateStore.ListAutomationPolicies(AutomationPolicyFilter{TenantID: tenantA.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(tenantPolicies) != 2 {
		t.Fatalf("tenant A policy count=%d, want 2", len(tenantPolicies))
	}
	projectPolicies, err := stateStore.ListAutomationPolicies(AutomationPolicyFilter{
		TenantID: tenantA.ID, ProjectID: projectA.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(projectPolicies) != 1 || projectPolicies[0].ID != created.ID {
		t.Fatalf("project policy list=%+v", projectPolicies)
	}
	adminProjectPolicies, err := stateStore.ListAutomationPolicies(AutomationPolicyFilter{
		ProjectID: projectB.ID, PlatformAdmin: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(adminProjectPolicies) != 1 || adminProjectPolicies[0].ID != tenantBPolicy.ID {
		t.Fatalf("admin project filter leaked or omitted policies: %+v", adminProjectPolicies)
	}
	if _, err := stateStore.GetAutomationPolicy(created.ID, tenantB.ID, false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-tenant get error=%v, want not found", err)
	}
	if adminPolicy, err := stateStore.GetAutomationPolicy(created.ID, tenantB.ID, true); err != nil || adminPolicy.ID != created.ID {
		t.Fatalf("platform-admin get should bypass tenant filter: policy=%+v err=%v", adminPolicy, err)
	}
	if _, err := stateStore.ListAutomationPolicies(AutomationPolicyFilter{}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("unscoped non-admin list error=%v, want invalid input", err)
	}

	update := created
	update.Name = "API Recovery Shadow"
	update.Mode = model.GatePolicyModeShadow
	update.Description = " evaluate only "
	updated, err := stateStore.UpdateAutomationPolicy(update, tenantA.ID, false, created.Generation)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Generation != 2 || updated.Mode != model.GatePolicyModeShadow || updated.Description != "evaluate only" {
		t.Fatalf("unexpected update result: %+v", updated)
	}
	if !updated.CreatedAt.Equal(created.CreatedAt) || !updated.UpdatedAt.After(created.UpdatedAt) {
		t.Fatalf("unexpected update timestamps: before=%+v after=%+v", created, updated)
	}
	if _, err := stateStore.UpdateAutomationPolicy(update, tenantA.ID, false, created.Generation); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale update error=%v, want conflict", err)
	}
	move := updated
	move.ProjectID = projectA2.ID
	if _, err := stateStore.UpdateAutomationPolicy(move, tenantA.ID, false, updated.Generation); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("parent mutation error=%v, want invalid input", err)
	}
	kindChange := updated
	kindChange.Kind = "future_kind"
	if _, err := stateStore.UpdateAutomationPolicy(kindChange, tenantA.ID, false, updated.Generation); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("kind mutation error=%v, want invalid input", err)
	}
	if _, err := stateStore.DeleteAutomationPolicy(updated.ID, tenantA.ID, false, created.Generation); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale delete error=%v, want conflict", err)
	}

	reopened := New(storePath)
	persisted, err := reopened.GetAutomationPolicy(updated.ID, tenantA.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Generation != updated.Generation || persisted.Name != updated.Name {
		t.Fatalf("reopened policy=%+v, want %+v", persisted, updated)
	}
	removed, err := reopened.DeleteAutomationPolicy(updated.ID, tenantA.ID, false, updated.Generation)
	if err != nil {
		t.Fatal(err)
	}
	if removed.ID != updated.ID {
		t.Fatalf("removed policy=%+v", removed)
	}
	if _, err := reopened.GetAutomationPolicy(updated.ID, tenantA.ID, false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted get error=%v, want not found", err)
	}
}

func TestAutomationPolicyJSONStoreRejectsUnsafePersistence(t *testing.T) {
	stateStore := New(filepath.Join(t.TempDir(), "store.json"))
	tenant, err := stateStore.CreateTenant("Automation Validation")
	if err != nil {
		t.Fatal(err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "Validation Project", "")
	if err != nil {
		t.Fatal(err)
	}
	otherTenant, err := stateStore.CreateTenant("Automation Validation Other")
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		mutate func(*model.AutomationPolicy)
	}{
		{
			name: "managed",
			mutate: func(policy *model.AutomationPolicy) {
				policy.Managed = true
			},
		},
		{
			name: "system owner",
			mutate: func(policy *model.AutomationPolicy) {
				policy.OwnerType = model.AutomationOwnerSystem
			},
		},
		{
			name: "managed kind",
			mutate: func(policy *model.AutomationPolicy) {
				policy.Kind = model.AutomationPolicyKindManagedSystem
			},
		},
		{
			name: "enforced mode",
			mutate: func(policy *model.AutomationPolicy) {
				policy.Mode = model.GatePolicyModeEnforced
			},
		},
		{
			name: "canary mode",
			mutate: func(policy *model.AutomationPolicy) {
				policy.Mode = model.GatePolicyModeCanary
			},
		},
		{
			name: "negative priority",
			mutate: func(policy *model.AutomationPolicy) {
				policy.Priority = -1
			},
		},
		{
			name: "priority exceeds Postgres integer",
			mutate: func(policy *model.AutomationPolicy) {
				policy.Priority = int(int64(2_147_483_647) + 1)
			},
		},
		{
			name: "empty rules",
			mutate: func(policy *model.AutomationPolicy) {
				policy.Rules = nil
			},
		},
		{
			name: "scope ID missing",
			mutate: func(policy *model.AutomationPolicy) {
				policy.Scope.ID = ""
			},
		},
		{
			name: "unknown trigger type",
			mutate: func(policy *model.AutomationPolicy) {
				policy.Rules[0].Trigger.Type = "unknown"
			},
		},
		{
			name: "invariant ID missing",
			mutate: func(policy *model.AutomationPolicy) {
				policy.Rules[0].Trigger.Type = model.AutomationTriggerInvariant
				policy.Rules[0].Trigger.InvariantID = ""
			},
		},
		{
			name: "duplicate rule IDs",
			mutate: func(policy *model.AutomationPolicy) {
				policy.Rules = append(policy.Rules, policy.Rules[0])
			},
		},
		{
			name: "missing safety contract",
			mutate: func(policy *model.AutomationPolicy) {
				policy.Rules[0].Safety.ActionContractID = ""
			},
		},
		{
			name: "negative trigger samples",
			mutate: func(policy *model.AutomationPolicy) {
				policy.Rules[0].Trigger.MinimumSamples = -1
			},
		},
		{
			name: "empty parameter key",
			mutate: func(policy *model.AutomationPolicy) {
				policy.Rules[0].Action.Parameters[" "] = "bad"
			},
		},
		{
			name: "duplicate normalized parameter key",
			mutate: func(policy *model.AutomationPolicy) {
				policy.Rules[0].Action.Parameters[" reason "] = "ambiguous"
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			policy := testAutomationPolicy(tenant.ID, project.ID, "Policy "+test.name)
			test.mutate(&policy)
			if _, err := stateStore.CreateAutomationPolicy(policy); !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("error=%v, want invalid input", err)
			}
		})
	}

	missingTenant := testAutomationPolicy("tenant_missing", "", "Missing Tenant")
	missingTenant.Scope.ID = "project_missing"
	if _, err := stateStore.CreateAutomationPolicy(missingTenant); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing tenant error=%v, want not found", err)
	}
	mismatchedProject := testAutomationPolicy(otherTenant.ID, project.ID, "Mismatched Project")
	if _, err := stateStore.CreateAutomationPolicy(mismatchedProject); !errors.Is(err, ErrNotFound) {
		t.Fatalf("mismatched project error=%v, want not found", err)
	}

	app, err := stateStore.CreateApp(tenant.ID, project.ID, "validation-app", "", model.AppSpec{
		Image: "ghcr.io/example/validation:latest", Replicas: 1, RuntimeID: "runtime_managed_shared",
	})
	if err != nil {
		t.Fatal(err)
	}
	appPolicy := testAutomationPolicy(tenant.ID, project.ID, "Valid App Scope")
	appPolicy.Scope = model.AutomationScope{Type: model.AutomationScopeApp, ID: app.ID}
	if _, err := stateStore.CreateAutomationPolicy(appPolicy); err != nil {
		t.Fatalf("valid app scope: %v", err)
	}
	missingApp := testAutomationPolicy(tenant.ID, project.ID, "Missing App Scope")
	missingApp.Scope = model.AutomationScope{Type: model.AutomationScopeApp, ID: "app_missing"}
	if _, err := stateStore.CreateAutomationPolicy(missingApp); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing app scope error=%v, want not found", err)
	}
	missingProject := testAutomationPolicy(tenant.ID, "", "Missing App Project")
	missingProject.Scope = model.AutomationScope{Type: model.AutomationScopeApp, ID: app.ID}
	if _, err := stateStore.CreateAutomationPolicy(missingProject); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("app scope without project error=%v, want invalid input", err)
	}
}

func TestAutomationPolicyJSONStoreCascadesTenantAndProjectDeletion(t *testing.T) {
	stateStore := New(filepath.Join(t.TempDir(), "store.json"))
	tenant, err := stateStore.CreateTenant("Automation Cascade")
	if err != nil {
		t.Fatal(err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "Cascade Project", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stateStore.CreateAutomationPolicy(testAutomationPolicy(tenant.ID, project.ID, "Project Policy")); err != nil {
		t.Fatal(err)
	}
	if _, err := stateStore.DeleteProject(project.ID); err != nil {
		t.Fatal(err)
	}
	policies, err := stateStore.ListAutomationPolicies(AutomationPolicyFilter{TenantID: tenant.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(policies) != 0 {
		t.Fatalf("project deletion left policies: %+v", policies)
	}

	tenantProject, err := stateStore.CreateProject(tenant.ID, "Tenant Delete Project", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stateStore.CreateAutomationPolicy(testAutomationPolicy(tenant.ID, tenantProject.ID, "Tenant Policy")); err != nil {
		t.Fatal(err)
	}
	if _, err := stateStore.DeleteTenant(tenant.ID); err != nil {
		t.Fatal(err)
	}
	policies, err = stateStore.ListAutomationPolicies(AutomationPolicyFilter{PlatformAdmin: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(policies) != 0 {
		t.Fatalf("tenant deletion left policies: %+v", policies)
	}
}

func TestAutomationPolicyJSONStoreFollowsAppProjectMoves(t *testing.T) {
	stateStore := New(filepath.Join(t.TempDir(), "store.json"))
	tenant, err := stateStore.CreateTenant("Automation App Move")
	if err != nil {
		t.Fatal(err)
	}
	sourceProject, err := stateStore.CreateProject(tenant.ID, "Source", "")
	if err != nil {
		t.Fatal(err)
	}
	targetProject, err := stateStore.CreateProject(tenant.ID, "Target", "")
	if err != nil {
		t.Fatal(err)
	}
	app, err := stateStore.CreateApp(tenant.ID, sourceProject.ID, "api", "", model.AppSpec{
		Image:     "ghcr.io/example/api:latest",
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})
	if err != nil {
		t.Fatal(err)
	}
	policy := testAutomationPolicy(tenant.ID, sourceProject.ID, "API Recovery")
	policy.Scope = model.AutomationScope{Type: model.AutomationScopeApp, ID: app.ID}
	created, err := stateStore.CreateAutomationPolicy(policy)
	if err != nil {
		t.Fatal(err)
	}

	dryRunPlan, err := stateStore.MoveAppProject(app.ID, AppProjectMoveOptions{
		TargetProjectID: targetProject.ID,
		DryRun:          true,
	})
	if err != nil {
		t.Fatalf("dry-run move app project: %v blockers=%v", err, dryRunPlan.Blockers)
	}
	if !strings.Contains(strings.Join(dryRunPlan.Warnings, "\n"), "automation policy API Recovery will move with app api") {
		t.Fatalf("dry-run plan did not disclose policy move: %+v", dryRunPlan.Warnings)
	}
	unchanged, err := stateStore.GetAutomationPolicy(created.ID, tenant.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	if unchanged.ProjectID != sourceProject.ID || unchanged.Generation != 1 {
		t.Fatalf("dry run changed policy: %+v", unchanged)
	}

	movePlan, err := stateStore.MoveAppProject(app.ID, AppProjectMoveOptions{
		TargetProjectID: targetProject.ID,
	})
	if err != nil {
		t.Fatalf("move app project: %v blockers=%v", err, movePlan.Blockers)
	}
	moved, err := stateStore.GetAutomationPolicy(created.ID, tenant.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	if moved.ProjectID != targetProject.ID || moved.Generation != 2 || !moved.UpdatedAt.After(created.UpdatedAt) {
		t.Fatalf("policy did not move with generation CAS boundary: before=%+v after=%+v", created, moved)
	}
	if _, err := stateStore.UpdateAutomationPolicy(moved, tenant.ID, false, created.Generation); !errors.Is(err, ErrConflict) {
		t.Fatalf("pre-move generation remained usable: %v", err)
	}
}

func TestAutomationPolicyJSONStoreBlocksProjectMoveNameConflict(t *testing.T) {
	stateStore := New(filepath.Join(t.TempDir(), "store.json"))
	tenant, err := stateStore.CreateTenant("Automation App Move Conflict")
	if err != nil {
		t.Fatal(err)
	}
	sourceProject, err := stateStore.CreateProject(tenant.ID, "Source", "")
	if err != nil {
		t.Fatal(err)
	}
	targetProject, err := stateStore.CreateProject(tenant.ID, "Target", "")
	if err != nil {
		t.Fatal(err)
	}
	sourceApp, err := stateStore.CreateApp(tenant.ID, sourceProject.ID, "source-api", "", model.AppSpec{
		Image: "ghcr.io/example/source:latest", Replicas: 1, RuntimeID: "runtime_managed_shared",
	})
	if err != nil {
		t.Fatal(err)
	}
	targetApp, err := stateStore.CreateApp(tenant.ID, targetProject.ID, "target-api", "", model.AppSpec{
		Image: "ghcr.io/example/target:latest", Replicas: 1, RuntimeID: "runtime_managed_shared",
	})
	if err != nil {
		t.Fatal(err)
	}
	sourcePolicy := testAutomationPolicy(tenant.ID, sourceProject.ID, "API Recovery")
	sourcePolicy.Scope = model.AutomationScope{Type: model.AutomationScopeApp, ID: sourceApp.ID}
	created, err := stateStore.CreateAutomationPolicy(sourcePolicy)
	if err != nil {
		t.Fatal(err)
	}
	targetPolicy := testAutomationPolicy(tenant.ID, targetProject.ID, "api recovery")
	targetPolicy.Scope = model.AutomationScope{Type: model.AutomationScopeApp, ID: targetApp.ID}
	if _, err := stateStore.CreateAutomationPolicy(targetPolicy); err != nil {
		t.Fatal(err)
	}

	plan, err := stateStore.MoveAppProject(sourceApp.ID, AppProjectMoveOptions{TargetProjectID: targetProject.ID})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("move conflict error=%v, want conflict", err)
	}
	if !strings.Contains(strings.Join(plan.Blockers, "\n"), "automation policy") {
		t.Fatalf("move conflict did not identify automation policies: %+v", plan.Blockers)
	}
	appAfter, err := stateStore.GetApp(sourceApp.ID)
	if err != nil {
		t.Fatal(err)
	}
	policyAfter, err := stateStore.GetAutomationPolicy(created.ID, tenant.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	if appAfter.ProjectID != sourceProject.ID || policyAfter.ProjectID != sourceProject.ID || policyAfter.Generation != 1 {
		t.Fatalf("blocked move partially mutated state: app=%+v policy=%+v", appAfter, policyAfter)
	}
}

func TestAutomationPolicyJSONStoreFollowsProjectSplit(t *testing.T) {
	stateStore := New(filepath.Join(t.TempDir(), "store.json"))
	tenant, err := stateStore.CreateTenant("Automation Project Split")
	if err != nil {
		t.Fatal(err)
	}
	sourceProject, err := stateStore.CreateProject(tenant.ID, "Source", "")
	if err != nil {
		t.Fatal(err)
	}
	targetProject, err := stateStore.CreateProject(tenant.ID, "Target", "")
	if err != nil {
		t.Fatal(err)
	}
	app, err := stateStore.CreateApp(tenant.ID, sourceProject.ID, "worker", "", model.AppSpec{
		Image: "ghcr.io/example/worker:latest", Replicas: 1, RuntimeID: "runtime_managed_shared",
	})
	if err != nil {
		t.Fatal(err)
	}
	policy := testAutomationPolicy(tenant.ID, sourceProject.ID, "Worker Recovery")
	policy.Scope = model.AutomationScope{Type: model.AutomationScopeApp, ID: app.ID}
	created, err := stateStore.CreateAutomationPolicy(policy)
	if err != nil {
		t.Fatal(err)
	}

	plan, err := stateStore.SplitProject(sourceProject.ID, ProjectSplitOptions{
		Targets: []ProjectSplitTarget{{
			AppID:           app.ID,
			TargetProjectID: targetProject.ID,
		}},
	})
	if err != nil {
		t.Fatalf("split project: %v blockers=%v", err, plan.Blockers)
	}
	moved, err := stateStore.GetAutomationPolicy(created.ID, tenant.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	if moved.ProjectID != targetProject.ID || moved.Generation != 2 {
		t.Fatalf("policy did not follow project split: %+v", moved)
	}
}

func TestAutomationPolicyJSONStorePurgesAppScopedPolicies(t *testing.T) {
	stateStore := New(filepath.Join(t.TempDir(), "store.json"))
	tenant, err := stateStore.CreateTenant("Automation App Purge")
	if err != nil {
		t.Fatal(err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "Imports", "")
	if err != nil {
		t.Fatal(err)
	}
	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "pending-import", "", model.AppSpec{
		Replicas: 1, RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:          model.AppSourceTypeGitHubPublic,
		RepoURL:       "https://github.com/example/pending-import",
		BuildStrategy: model.AppBuildStrategyBuildpacks,
	}, model.AppRoute{})
	if err != nil {
		t.Fatal(err)
	}
	policy := testAutomationPolicy(tenant.ID, project.ID, "Pending Import Recovery")
	policy.Scope = model.AutomationScope{Type: model.AutomationScopeApp, ID: app.ID}
	created, err := stateStore.CreateAutomationPolicy(policy)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := stateStore.PurgeApp(app.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := stateStore.GetAutomationPolicy(created.ID, tenant.ID, false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("purged app policy error=%v, want not found", err)
	}
}

func TestDefaultControlPlanePermissionAuditIncludesAutomationPolicyCRUD(t *testing.T) {
	t.Parallel()

	grants := strings.Join(defaultControlPlaneRequiredGrants(), "\n")
	for _, required := range []string{
		"fugue_automation_policies:select",
		"fugue_automation_policies:insert",
		"fugue_automation_policies:update",
		"fugue_automation_policies:delete",
	} {
		if !strings.Contains(grants, required) {
			t.Fatalf("default permission audit is missing %q", required)
		}
	}
}

func TestAutomationPolicyJSONStoreFailsClosedOnVisibleCorruption(t *testing.T) {
	stateStore := New(filepath.Join(t.TempDir(), "store.json"))
	tenantA, err := stateStore.CreateTenant("Automation Corruption A")
	if err != nil {
		t.Fatal(err)
	}
	projectA, err := stateStore.CreateProject(tenantA.ID, "Corruption Project A", "")
	if err != nil {
		t.Fatal(err)
	}
	tenantB, err := stateStore.CreateTenant("Automation Corruption B")
	if err != nil {
		t.Fatal(err)
	}
	projectB, err := stateStore.CreateProject(tenantB.ID, "Corruption Project B", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stateStore.CreateAutomationPolicy(testAutomationPolicy(tenantA.ID, projectA.ID, "Healthy Policy")); err != nil {
		t.Fatal(err)
	}
	corrupt, err := stateStore.CreateAutomationPolicy(testAutomationPolicy(tenantB.ID, projectB.ID, "Corrupt Policy"))
	if err != nil {
		t.Fatal(err)
	}
	if err := stateStore.withLockedState(true, func(state *model.State) error {
		index := findAutomationPolicyByID(state.AutomationPolicies, corrupt.ID)
		if index < 0 {
			return ErrNotFound
		}
		state.AutomationPolicies[index].Mode = model.GatePolicyModeEnforced
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	policies, err := stateStore.ListAutomationPolicies(AutomationPolicyFilter{TenantID: tenantA.ID})
	if err != nil {
		t.Fatalf("another tenant's corrupt policy must not break scoped reads: %v", err)
	}
	if len(policies) != 1 {
		t.Fatalf("tenant A policy count=%d, want 1", len(policies))
	}
	if _, err := stateStore.ListAutomationPolicies(AutomationPolicyFilter{TenantID: tenantB.ID}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("corrupt policy list error=%v, want invalid input", err)
	}
	if _, err := stateStore.GetAutomationPolicy(corrupt.ID, tenantB.ID, false); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("corrupt policy get error=%v, want invalid input", err)
	}
	if _, err := stateStore.DeleteAutomationPolicy(corrupt.ID, tenantB.ID, false, corrupt.Generation); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("corrupt policy delete error=%v, want invalid input", err)
	}
}

func TestPostgresSchemaIncludesUserAutomationPolicySafetyBoundary(t *testing.T) {
	t.Parallel()

	schema := strings.Join(postgresSchemaStatements, "\n")
	for _, required := range []string{
		"CREATE TABLE IF NOT EXISTS fugue_automation_policies",
		"id TEXT PRIMARY KEY CHECK (btrim(id) <> '')",
		"tenant_id TEXT NOT NULL REFERENCES fugue_tenants(id) ON DELETE CASCADE",
		"CREATE UNIQUE INDEX IF NOT EXISTS idx_fugue_projects_id_tenant",
		"FOREIGN KEY (project_id, tenant_id) REFERENCES fugue_projects(id, tenant_id) ON DELETE CASCADE",
		"kind TEXT NOT NULL CHECK (lower(kind) <> 'managed_system')",
		"owner_type TEXT NOT NULL CHECK (owner_type = 'user')",
		"scope_id TEXT NOT NULL DEFAULT '' CHECK (lower(scope_type) = 'cluster' OR btrim(scope_id) <> '')",
		"mode TEXT NOT NULL CHECK (mode IN ('disabled', 'shadow'))",
		"priority INTEGER NOT NULL DEFAULT 0 CHECK (priority >= 0)",
		"managed BOOLEAN NOT NULL DEFAULT FALSE CHECK (managed = FALSE)",
		"rules_json JSONB NOT NULL CHECK (",
		"WHEN jsonb_typeof(rules_json) = 'array' THEN jsonb_array_length(rules_json) > 0",
		"generation BIGINT NOT NULL CHECK (generation > 0)",
		"metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata_json) = 'object')",
		"idx_fugue_automation_policies_scope_name",
		"idx_fugue_automation_policies_tenant_updated",
		"idx_fugue_automation_policies_project_updated",
		"idx_fugue_automation_policies_scope",
	} {
		if !strings.Contains(schema, required) {
			t.Fatalf("postgres schema is missing automation persistence boundary %q", required)
		}
	}
}

func testAutomationPolicy(tenantID, projectID, name string) model.AutomationPolicy {
	return model.AutomationPolicy{
		TenantID:  tenantID,
		ProjectID: projectID,
		Name:      name,
		Kind:      model.AutomationPolicyKindAppRecovery,
		OwnerType: model.AutomationOwnerUser,
		Scope: model.AutomationScope{
			Type: "project",
			ID:   projectID,
		},
		Mode:     model.GatePolicyModeDisabled,
		Priority: 100,
		Managed:  false,
		Rules: []model.AutomationRule{{
			ID: "restart-on-unavailability",
			Trigger: model.AutomationTrigger{
				Type:                  model.AutomationTriggerRequestMetric,
				Source:                "request_outcomes",
				RequiredEvidence:      []string{" http_status ", "failure_domain", "http_status"},
				MinimumSamples:        3,
				MinimumFailureDomains: 2,
			},
			Action: model.AutomationAction{
				Type: "restart_app",
				Parameters: map[string]string{
					"reason": " restart unhealthy app ",
				},
			},
			Safety: model.AutomationSafetyPolicy{
				ActionContractID:       "app.restart",
				GatePolicyID:           "automation.app-restart",
				TTL:                    "5m",
				RecoveryCondition:      "application request probes are healthy",
				RollbackAction:         "restore previous application revision",
				RequiresRollbackTarget: true,
				RequiresAudit:          true,
				RequiresWAL:            true,
				RequiresIdempotencyKey: true,
				RequiresFencingToken:   true,
			},
		}},
		Metadata: map[string]string{"team": " platform "},
	}
}
