package main

import (
	"testing"
)

func TestParseTargetsIsDataDrivenAndGroupScoped(t *testing.T) {
	targets, err := parseTargets("edge-control,edge-pool-a,fugue-system,edge-control-alpha,fugue-fugue;edge-control,edge-pool-b,fugue-system,edge-control-beta,fugue-fugue")
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 2 || targets[0].Key.String() == targets[1].Key.String() {
		t.Fatalf("targets=%+v", targets)
	}
}

func TestParseCanaryRejectsUnboundedOrMalformedInput(t *testing.T) {
	if _, err := parseProbe("edge-control,edge-pool-a,192.0.2.1:443,platform.example,/healthz,ok,10"); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{
		"edge-control,edge-pool-a,192.0.2.1:443,platform.example,/healthz,ok,1",
		"edge-control,edge-pool-a,192.0.2.1:443,platform.example,healthz,ok,10",
		"edge-control,edge-pool-a,192.0.2.1:443,platform.example,/healthz,ok\nleak,10",
	} {
		if _, err := parseProbe(value); err == nil {
			t.Fatalf("accepted %q", value)
		}
	}
}
