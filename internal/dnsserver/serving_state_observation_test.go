package dnsserver

import (
	"sync"
	"testing"

	"fugue/internal/dnsfacts"
)

// Fixtures do not model a real publication, so never assign them a writer.
func (p *dnsServingPointer) Store(state *dnsServingState) {
	p.publish(state, "")
}

func TestDNSServingPublisherRetainsOriginalPointerEvenOnRollback(t *testing.T) {
	s := &Service{}
	old, candidate := &dnsServingState{}, &dnsServingState{}
	s.platformServing.publish(old, dnsServingWriterCheckpoint)
	s.platformServing.publish(candidate, dnsServingWriterCandidate)
	before := s.currentDNSServingSelection()
	s.platformServing.publish(old, dnsServingWriterRollback)
	after := s.currentDNSServingSelection()
	if before.state != candidate || after.state != old || s.platformServing.Load() != old {
		t.Fatal("diagnostic publication wrapper changed serving pointer identity")
	}
	if writer, path, renewal := dnsServingReplacement(before, after); writer != dnsfacts.ReplacementRollback || path != dnsfacts.ReplacementBaseToBase || renewal != dnsfacts.ReplacementUnchanged {
		t.Fatalf("unexpected rollback attribution %s %s %s", writer, path, renewal)
	}
}

func TestDNSServingReplacementRecordsBothObservedInputs(t *testing.T) {
	a, b := &dnsServingPublication{writer: dnsServingWriterReadinessIncomplete}, &dnsServingPublication{writer: dnsServingWriterCandidate}
	x, y := &dnsServingRenewal{}, &dnsServingRenewal{}
	for _, c := range []struct {
		name             string
		before, after    dnsServingSelection
		base, path, scan string
	}{
		{"new renewal", dnsServingSelection{base: a, renewal: x}, dnsServingSelection{base: a, renewal: y, renewalSelected: true}, dnsfacts.ReplacementUnchanged, dnsfacts.ReplacementBaseToRenewal, dnsfacts.ReplacementChanged},
		{"renewal displaced", dnsServingSelection{base: a, renewal: x, renewalSelected: true}, dnsServingSelection{base: b, renewal: x}, dnsfacts.ReplacementCandidate, dnsfacts.ReplacementRenewalToBase, dnsfacts.ReplacementUnchanged},
		{"both changed", dnsServingSelection{base: a, renewal: x, renewalSelected: true}, dnsServingSelection{base: b, renewal: y, renewalSelected: true}, dnsfacts.ReplacementCandidate, dnsfacts.ReplacementRenewalToRenewal, dnsfacts.ReplacementChanged},
		{"missing attribution", dnsServingSelection{base: a}, dnsServingSelection{base: &dnsServingPublication{}}, dnsfacts.ReplacementUnattributed, dnsfacts.ReplacementBaseToBase, dnsfacts.ReplacementUnchanged},
	} {
		t.Run(c.name, func(t *testing.T) {
			if base, path, renewal := dnsServingReplacement(c.before, c.after); base != c.base || path != c.path || renewal != c.scan {
				t.Fatalf("observed %s %s %s; expected %s %s %s", base, path, renewal, c.base, c.path, c.scan)
			}
		})
	}
}

func TestDNSServingPublicationWriterCannotRaceWithItsPointer(t *testing.T) {
	s := &Service{}
	old, candidate := &dnsServingState{}, &dnsServingState{}
	s.platformServing.publish(old, dnsServingWriterRollback)
	var workers sync.WaitGroup
	for range 2 {
		workers.Go(func() {
			for range 1000 {
				s.platformServing.publish(candidate, dnsServingWriterCandidate)
				s.platformServing.publish(old, dnsServingWriterRollback)
			}
		})
	}
	for range 2000 {
		seen := s.currentDNSServingSelection()
		if !(seen.state == old && seen.base.writer == dnsServingWriterRollback || seen.state == candidate && seen.base.writer == dnsServingWriterCandidate) {
			t.Error("attributed a state to a different atomic publication")
			break
		}
	}
	workers.Wait()
}
