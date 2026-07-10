package model

import "testing"

func TestKernelBreakGlassScopeMustBeExplicit(t *testing.T) {
	platformAdmin := Principal{
		ActorType: ActorTypeAPIKey,
		Scopes:    map[string]struct{}{"platform.admin": {}},
	}
	if !platformAdmin.HasScope("artifact.kernel_break_glass") {
		t.Fatal("platform.admin should retain ordinary inherited administration scopes")
	}
	if platformAdmin.HasExplicitScope("artifact.kernel_break_glass") {
		t.Fatal("platform.admin must not inherit kernel break-glass authority")
	}

	explicit := platformAdmin
	explicit.Scopes = map[string]struct{}{
		"platform.admin":              {},
		"artifact.kernel_break_glass": {},
	}
	if !explicit.HasExplicitScope("artifact.kernel_break_glass") {
		t.Fatal("explicit kernel break-glass scope was not recognized")
	}

	bootstrap := Principal{ActorType: ActorTypeBootstrap}
	if !bootstrap.HasExplicitScope("artifact.kernel_break_glass") {
		t.Fatal("bootstrap recovery identity must retain explicit emergency authority")
	}
}
