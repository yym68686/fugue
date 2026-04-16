package workloadidentity

import "testing"

func TestIssueAndParseRoundTrip(t *testing.T) {
	t.Parallel()

	token, err := Issue("signing-secret", Claims{
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		AppID:     "app_demo",
		Scopes:    []string{"app.write", "app.deploy", "app.delete"},
	})
	if err != nil {
		t.Fatalf("issue token: %v", err)
	}
	claims, err := Parse("signing-secret", token)
	if err != nil {
		t.Fatalf("parse token: %v", err)
	}
	if claims.TenantID != "tenant_demo" {
		t.Fatalf("expected tenant id tenant_demo, got %q", claims.TenantID)
	}
	if claims.ProjectID != "project_demo" {
		t.Fatalf("expected project id project_demo, got %q", claims.ProjectID)
	}
	if claims.AppID != "app_demo" {
		t.Fatalf("expected app id app_demo, got %q", claims.AppID)
	}
	if len(claims.Scopes) != 3 {
		t.Fatalf("expected 3 scopes, got %d", len(claims.Scopes))
	}
}

func TestParseRejectsWrongSigningKey(t *testing.T) {
	t.Parallel()

	token, err := Issue("signing-secret", Claims{
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
	})
	if err != nil {
		t.Fatalf("issue token: %v", err)
	}
	if _, err := Parse("other-secret", token); err == nil {
		t.Fatal("expected parse with wrong signing key to fail")
	}
}

func TestIssueWithoutIssuedAtIsDeterministic(t *testing.T) {
	t.Parallel()

	claims := Claims{
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		AppID:     "app_demo",
		Scopes:    []string{"app.write", "app.deploy", "app.delete"},
	}
	first, err := Issue("signing-secret", claims)
	if err != nil {
		t.Fatalf("issue first token: %v", err)
	}
	second, err := Issue("signing-secret", claims)
	if err != nil {
		t.Fatalf("issue second token: %v", err)
	}
	if first != second {
		t.Fatalf("expected deterministic token without explicit issued_at, got %q != %q", first, second)
	}

	parsed, err := Parse("signing-secret", first)
	if err != nil {
		t.Fatalf("parse deterministic token: %v", err)
	}
	if parsed.IssuedAt != 0 {
		t.Fatalf("expected issued_at to remain unset by default, got %d", parsed.IssuedAt)
	}
}
