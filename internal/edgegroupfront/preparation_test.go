package edgegroupfront

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPublicationPreparationPreservesTrafficAndFencesActivation(t *testing.T) {
	now := time.Now().UTC()
	path := filepath.Join(t.TempDir(), "activation.json")
	initial, err := ApplyActivationCAS(path, activationRequest(ActivationOperationInit, 0, "a", "a", "old.p1.r0", 0), now)
	if err != nil {
		t.Fatal(err)
	}
	request := activationRequest(ActivationOperationPromote, 1, "a", "b", "next.p2.r4", 0)
	request.RequirePublicationPreparation = true
	if _, err := ApplyActivationCAS(path, request, now); err == nil {
		t.Fatal("activation accepted without preparation")
	}
	before, _ := os.ReadFile(path)
	p, err := PreparePublication(path, request, now)
	if err != nil {
		t.Fatal(err)
	}
	again, err := PreparePublication(path, request, now.Add(time.Second))
	if err != nil || again != p {
		t.Fatal("preparation replay renewed the original deadline", err)
	}
	after, _ := os.ReadFile(path)
	if string(before) != string(after) || !p.Matches(initial.Current, "b", testActivationCommit, now) {
		t.Fatal("preparation mutated traffic or lost predecessor")
	}
	wrong := request
	wrong.BundleGeneration = "other.p3.r4"
	if _, err := PreparePublication(path, wrong, now); err == nil {
		t.Fatal("conflicting preparation replaced live record")
	}
	if _, err := ApplyActivationCAS(path, wrong, now); err == nil {
		t.Fatal("preparation granted different publication")
	}
	if _, err := ApplyActivationCAS(path, request, p.ExpiresAt); err == nil {
		t.Fatal("expired preparation activated traffic")
	}
	activated, err := ApplyActivationCAS(path, request, now.Add(time.Second))
	if err != nil || activated.Current.ActiveSlot != "b" {
		t.Fatal("prepared activation failed", err)
	}
	if p.Matches(activated.Current, "b", testActivationCommit, now.Add(time.Second)) {
		t.Fatal("old preparation survived activation fence")
	}
	rollback := activationRequest(ActivationOperationRollback, 2, "b", "a", "old.p1.r0", 2)
	if _, err := ApplyActivationCAS(path, rollback, now.Add(2*time.Second)); err != nil {
		t.Fatal("configuration recovery depends on preparation", err)
	}
}

func TestPublicationPreparationRejectsChangedPredecessorAndIdentity(t *testing.T) {
	now := time.Now().UTC()
	path := filepath.Join(t.TempDir(), "activation.json")
	initial, err := ApplyActivationCAS(path, activationRequest(ActivationOperationInit, 0, "a", "a", "old.p1.r0", 0), now)
	if err != nil {
		t.Fatal(err)
	}
	p, err := PreparePublication(path, activationRequest(ActivationOperationPromote, 1, "a", "b", "next.p2.r4", 0), now)
	if err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*ActivationState){
		"generation": func(s *ActivationState) { s.Generation++ }, "group": func(s *ActivationState) { s.GroupID = "edge-group-other" },
		"slot": func(s *ActivationState) { s.ActiveSlot = "b" }, "bundle": func(s *ActivationState) { s.BundleGeneration = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			s := initial.Current
			change(&s)
			if p.Matches(s, "b", testActivationCommit, now) {
				t.Fatal("changed predecessor accepted")
			}
		})
	}
	if p.Matches(initial.Current, "a", testActivationCommit, now) || p.Matches(initial.Current, "b", "different-code", now) || p.Matches(initial.Current, "b", testActivationCommit, now.Add(-time.Second)) {
		t.Fatal("foreign or future preparation accepted")
	}
	if err := os.Chmod(path+".prepared", 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ReadPublicationPreparation(path); err == nil {
		t.Fatal("nonprivate preparation accepted")
	}
}
