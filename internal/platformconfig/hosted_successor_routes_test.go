package platformconfig

import (
	"encoding/json"
	"maps"
	"reflect"
	"testing"

	"fugue/internal/model"
)

func testHostedSuccessorWire(t *testing.T, start, stage, desired model.PlatformArtifact, p HostedScopeProposal) {
	t.Helper()
	withRows := func(a model.PlatformArtifact, v HostedSuccessorRoutes) model.PlatformArtifact {
		c := maps.Clone(a.Content)
		c["routes"] = v.Rows
		delete(c, managedHostDNSBindingsField)
		if len(v.ManagedBindings) != 0 {
			c[managedHostDNSBindingsField] = v.ManagedBindings
		}
		return model.PlatformArtifact{ArtifactKind: a.ArtifactKind, Scope: a.Scope, ScopeKey: a.ScopeKey, Content: c}
	}
	x, e := ComposeHostedSuccessorRoutes(start, stage, p, "prepare")
	if e != nil {
		t.Fatal("prepare ordinary route encoding", e)
	}
	prepared := withRows(start, x)
	y, e := ComposeHostedSuccessorRoutes(prepared, desired, p, "forward")
	if e != nil {
		t.Fatal("forward ordinary route encoding", e)
	}
	active := withRows(prepared, y)
	z, e := ComposeHostedSuccessorRoutes(active, stage, p, "withdraw")
	if e != nil || !reflect.DeepEqual(x, z) {
		t.Fatal("withdraw did not restore exact prepared wire form", e)
	}
	broken := active
	broken.Content = maps.Clone(active.Content)
	delete(broken.Content, managedHostDNSBindingsField)
	if _, e := ComposeHostedSuccessorRoutes(broken, stage, p, "withdraw"); e == nil {
		t.Fatal("accepted missing active managed DNS owner")
	}
	broken = desired
	broken.Content = maps.Clone(desired.Content)
	broken.Content["edge_selection_grants"] = []any{map[string]any{"hostname": "other.example.test"}}
	if got, e := ComposeHostedSuccessorRoutes(prepared, broken, p, "forward"); e != nil || !reflect.DeepEqual(got, y) {
		t.Fatal("an unrelated grant changed the host's wire rows or blocked composition", e)
	}
	// Model another host entering after the forward release. A reverse
	// release retains both the row and its latest upstream, not a snapshot
	// of the group taken before that tenant's deployment.
	var foreign map[string]any
	if err := json.Unmarshal(y.Rows[0], &foreign); err != nil {
		t.Fatal(err)
	}
	foreign["hostname"] = "unrelated.example.test"
	b, _ := json.Marshal(foreign)
	newer := y
	newer.Rows = append([]json.RawMessage{b}, y.Rows...)
	retained, e := ComposeHostedSuccessorRoutes(withRows(active, newer), stage, p, "withdraw")
	if e != nil || len(retained.Rows) != len(x.Rows)+1 || string(retained.Rows[0]) != string(b) {
		t.Fatal("rewound an unrelated route", e)
	}
}
