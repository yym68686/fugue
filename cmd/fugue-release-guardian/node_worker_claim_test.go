package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"fugue/internal/edgegroupfront"
	"fugue/internal/releaseguardian"
)

func TestNodeWorkerClaimWaitsOnlyForProvenPrewriteContention(t *testing.T) {
	ambiguous := errors.New("Lease update reply lost")
	for _, tc := range []struct {
		name            string
		first, want     error
		recent, expired bool
		calls           int
	}{
		{name: "busy then available", first: releaseguardian.ErrNodeWorkerComponentBusy, recent: true, calls: 2},
		{name: "unknown write outcome", first: ambiguous, want: ambiguous, recent: true, calls: 1},
		{name: "bounded", first: releaseguardian.ErrNodeWorkerComponentBusy, want: releaseguardian.ErrNodeWorkerComponentBusy, recent: true, expired: true, calls: 1},
		{name: "freshness lost during contention", first: releaseguardian.ErrNodeWorkerComponentBusy, calls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			wait := time.Second
			if tc.expired {
				wait = 0
			}
			_, err := waitNodeWorkerComponent(context.Background(), func() bool { return calls == 0 || tc.recent },
				wait, time.Millisecond, func() (releaseguardian.NodeWorkerComponentHold, error) {
					calls++
					if calls == 1 {
						return releaseguardian.NodeWorkerComponentHold{}, tc.first
					}
					return releaseguardian.NodeWorkerComponentHold{}, nil
				})
			if calls != tc.calls || (tc.recent && !errors.Is(err, tc.want)) ||
				(!tc.recent && (err == nil || !strings.Contains(err.Error(), "recent observations"))) {
				t.Fatalf("calls=%d err=%v", calls, err)
			}
		})
	}
}

func TestNodeWorkerSealOutputsExactlyTheClaimWireFormat(t *testing.T) {
	_, _, journal := nodeWorkerRuntimeFixture(t)
	want := journal.Digest
	journal.Digest = ""
	unsigned, err := json.MarshalIndent(journal, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := runNodeWorkerSeal(nil, bytes.NewReader(unsigned), &output); err != nil {
		t.Fatal(err)
	}
	parsed, err := edgegroupfront.ReadNodeWorkerJournal(output.Bytes())
	if err != nil || parsed.Digest != want {
		t.Fatalf("release job cannot retain the canonical sealed claim: %v %s", err, parsed.Digest)
	}
	for _, malformed := range []string{
		output.String(),
		string(unsigned) + "{}",
		strings.Replace(string(unsigned), "\"node_uid\"", "\"unexpected\"", 1),
		strings.Repeat(" ", (16<<10)+1) + string(unsigned),
	} {
		output.Reset()
		if err := runNodeWorkerSeal(nil, strings.NewReader(malformed), &output); err == nil || output.Len() != 0 {
			t.Fatal("invalid journal produced claim bytes")
		}
	}
}
