package store

import (
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestStaticEdgeRegistrationTenantAndProjectBoundaries(t *testing.T) {
	t.Parallel()
	s := New(t.TempDir() + "/state.json")
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	owner, err := s.CreateTenant("owner")
	if err != nil {
		t.Fatal(err)
	}
	other, err := s.CreateTenant("other")
	if err != nil {
		t.Fatal(err)
	}
	project, err := s.CreateProject(owner.ID, "project", "")
	if err != nil {
		t.Fatal(err)
	}
	otherProject, err := s.CreateProject(other.ID, "project", "")
	if err != nil {
		t.Fatal(err)
	}
	registration := model.StaticEdgeRegistration{
		TenantID: owner.ID, ProjectID: project.ID, Name: "edge", EdgeID: "edge-one",
		Transport: model.StaticEdgeTransportMTLS, SigningKeyID: "key-one",
		PossessionProofDigest: "sha256:" + strings.Repeat("a", 64),
	}
	if _, err := s.CreateStaticEdgeRegistration(model.StaticEdgeRegistration{
		TenantID: owner.ID, ProjectID: otherProject.ID, Name: "cross-tenant", EdgeID: "cross-tenant",
		Transport: model.StaticEdgeTransportMTLS, SigningKeyID: "key-one",
		PossessionProofDigest: registration.PossessionProofDigest,
	}); err != ErrNotFound {
		t.Fatalf("cross-tenant project must be rejected, got %v", err)
	}
	if _, err := s.CreateStaticEdgeRegistration(model.StaticEdgeRegistration{
		TenantID: owner.ID, ProjectID: project.ID, Name: "missing-proof", EdgeID: "missing-proof",
		Transport: model.StaticEdgeTransportMTLS,
	}); err == nil {
		t.Fatal("missing observation digest was accepted")
	}
	created, err := s.CreateStaticEdgeRegistration(registration)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateStaticEdgeRegistration(registration); err != ErrConflict {
		t.Fatalf("duplicate edge identity must conflict, got %v", err)
	}
	for _, filter := range []StaticEdgeRegistrationFilter{
		{TenantID: other.ID, PlatformAdmin: true},
		{TenantID: other.ID},
		{TenantID: owner.ID, ProjectID: otherProject.ID, PlatformAdmin: true},
	} {
		listed, err := s.ListStaticEdgeRegistrations(filter)
		if err != nil || listed == nil || len(listed) != 0 {
			t.Fatalf("filtered list must be an empty array: filter=%+v registrations=%+v err=%v", filter, listed, err)
		}
	}
	visible, err := s.ListStaticEdgeRegistrations(StaticEdgeRegistrationFilter{TenantID: owner.ID, PlatformAdmin: true})
	if err != nil || len(visible) != 1 || visible[0].ID != created.ID {
		t.Fatalf("owner registration missing: registrations=%+v err=%v", visible, err)
	}
	if _, err := s.GetStaticEdgeRegistration(created.ID, other.ID, false); err != ErrNotFound {
		t.Fatalf("other tenant read must be hidden, got %v", err)
	}
}
