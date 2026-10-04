package cli

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestStaticEdgeCutoverHistoricalQUICFailurePreventsDNSWrites(t *testing.T) {
	for _, advertisement := range []string{"", "clear", `h3=":443"`} {
		t.Run(advertisement, func(t *testing.T) {
			fixture, options := newCutoverFixture(t)
			options.RequiredHTTP3Ports = []int{31443}
			var calls []int
			tcpProbe := func(context.Context, string, string, string, string, int, string, time.Duration, ...bool) (staticEdgeProbeResult, error) {
				return staticEdgeProbeResult{Status: 200, AltSvc: advertisement}, nil
			}
			quicProbe := func(_ context.Context, _ string, _ string, port int, _ int, _ string, _ string, _ time.Duration) error {
				calls = append(calls, port)
				if port == 31443 {
					return errors.New("cached QUIC port unavailable")
				}
				return nil
			}
			check := func(ctx context.Context, o staticEdgeCutoverOptions) error {
				return probeStaticEdgeEndpoints(ctx, o, "candidate", true, tcpProbe, quicProbe)
			}
			cli := newCLI(&bytes.Buffer{}, &bytes.Buffer{})
			err := cli.runStaticEdgeCutoverWithChecks(context.Background(), options, goodCutoverCheck, check, goodCutoverCheck)
			if err == nil || len(calls) == 0 || calls[len(calls)-1] != 31443 || len(fixture.writes) != 0 {
				t.Fatalf("cached port lost when current Alt-Svc=%q: err=%v calls=%v writes=%v", advertisement, err, calls, fixture.writes)
			}
		})
	}
}

func TestStaticEdgeHistoricalQUICRequirementsPersistAndChangeIntentIdentity(t *testing.T) {
	_, o := newCutoverFixture(t)
	base, err := normalizeStaticEdgeCutover(o)
	if err != nil {
		t.Fatal(err)
	}
	o.RequiredHTTP3Ports = []int{31443, 443, 443}
	required, err := normalizeStaticEdgeCutover(o)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(required.RequiredHTTP3Ports, []int{443, 31443}) || staticEdgeCutoverID(required) == staticEdgeCutoverID(base) {
		t.Fatal("historical coverage not bound to intent")
	}
	if _, err := runCutoverFixture(required); err != nil {
		t.Fatal(err)
	}
	journal, err := readStaticEdgeCutoverJournal(staticEdgeCutoverID(required))
	if err != nil || !reflect.DeepEqual(journal.Options.RequiredHTTP3Ports, required.RequiredHTTP3Ports) {
		t.Fatalf("history lost on resume: %+v %v", journal, err)
	}
	for _, port := range []int{0, -1, 65536} {
		o.RequiredHTTP3Ports = []int{port}
		if _, err := normalizeStaticEdgeCutover(o); err == nil {
			t.Fatalf("accepted port %d", port)
		}
	}
}
