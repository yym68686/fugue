package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"fugue/internal/declarativerelease"
)

func TestEdgeGroupDrainCannotBeBypassedByAnAutomaticRoll(t *testing.T) {
	transition := edgeTransitionFixture()
	resource := func(name, strategy string) string {
		return fmt.Sprintf(`{"apiVersion":"apps/v1","kind":"DaemonSet","metadata":{"name":%q,"namespace":"test-system"},"spec":{"updateStrategy":{"type":%q}}}`, name, strategy)
	}
	for _, strategy := range []string{"OnDelete", "RollingUpdate", ""} {
		items := []string{resource(transition.FrontName, "OnDelete"), resource(transition.WorkerAName, "OnDelete"), resource(transition.WorkerBName, strategy)}
		manifest := []byte(`{"apiVersion":"release.fugue.dev/v2","kind":"ComponentResourceSet","items":[` + strings.Join(items, ",") + `]}`)
		if err := requireEdgeGroupOnDelete("test-system", transition, manifest); (err == nil) != (strategy == "OnDelete") {
			t.Fatalf("strategy %q: %v", strategy, err)
		}
	}
}

func TestVerifyEdgeWorkerDrainRequiresStableInactiveSlotAndNoSockets(t *testing.T) {
	now := time.Now().UTC()
	for _, tc := range []struct {
		name       string
		active     string
		age        time.Duration
		sockets    int
		socketErr  error
		race       bool
		want       string
		readSocket bool
	}{
		{name: "drained", active: "b", age: time.Minute, readSocket: true},
		{name: "active", active: "a", age: time.Minute, want: "stable inactive"},
		{name: "recently switched", active: "b", age: time.Second, want: "stable inactive"},
		{name: "invalid clock", active: "b", age: -time.Second, want: "stable inactive"},
		{name: "original connection remains after promotion or rollback", active: "b", age: time.Minute, sockets: 1, want: "live TCP sockets", readSocket: true},
		{name: "inventory unavailable", active: "b", age: time.Minute, socketErr: errors.New("read failed"), want: "socket observation unavailable", readSocket: true},
		{name: "concurrent activation", active: "b", age: time.Minute, race: true, want: "activation changed", readSocket: true},
		{name: "busy slot changed while observing", active: "b", age: time.Minute, sockets: 6, race: true, want: "activation changed", readSocket: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original := edgeActivationState{ActiveSlot: tc.active, Generation: 3, UpdatedAt: now.Add(-tc.age)}
			activationReads, socketReads := 0, 0
			err := verifyEdgeWorkerDrain("a", now, func() (edgeActivationState, bool, error) {
				activationReads++
				if tc.race && activationReads > 1 {
					original.ActiveSlot = "a"
					original.Generation++
				}
				return original, true, nil
			}, func() (int, error) {
				socketReads++
				return tc.sockets, tc.socketErr
			})
			if tc.want == "" && err != nil || tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) {
				t.Fatalf("drain result=%v, want %q", err, tc.want)
			}
			if (socketReads == 1) != tc.readSocket {
				t.Fatalf("socket inventory read %d times, expected %t", socketReads, tc.readSocket)
			}
			if errors.Is(err, declarativerelease.ErrInactiveWorkerDraining) != (tc.sockets > 0 && !tc.race) {
				t.Fatalf("unsafe wait classification: %v", err)
			}
		})
	}
}

func TestDrainingMemberDoesNotBlockIndependentReplacement(t *testing.T) {
	nodes := []string{"first", "second", "third"}
	for _, uncertain := range []bool{false, true} {
		var visited []string
		err := visitEdgeRollNodes(nodes, func(n string) error {
			visited = append(visited, n)
			if n == "first" {
				return declarativerelease.ErrInactiveWorkerDraining
			}
			if n == "second" && uncertain {
				return errors.New("incomplete socket inventory")
			}
			return nil
		})
		if err == nil || errors.Is(err, declarativerelease.ErrInactiveWorkerDraining) == uncertain ||
			!uncertain && !slices.Equal(visited, nodes) || uncertain && !slices.Equal(visited, nodes[:2]) {
			t.Fatalf("uncertain=%t visited=%v err=%v", uncertain, visited, err)
		}
	}
}

func TestEdgeDrainContinuesOnlyThePendingMembersOfSameAttempt(t *testing.T) {
	var visited []string
	remaining := map[string]int{"first": 2, "second": 1}
	waits := 0
	err := watchEdgeRollNodes([]string{"first", "second", "third"}, func() bool { waits++; return waits <= 2 }, func(node string) error {
		visited = append(visited, node)
		if remaining[node] > 0 {
			remaining[node]--
			return declarativerelease.ErrInactiveWorkerDraining
		}
		return nil
	})
	if err != nil || waits != 2 || !slices.Equal(visited, []string{"first", "second", "third", "first", "second", "first"}) {
		t.Fatal("missed a member or retried completed work", visited, err)
	}
}

func TestEdgeWorkerSocketInventoryIncludesBothFamiliesAndFailsClosed(t *testing.T) {
	const header = "  sl  local_address rem_address   st tx_queue rx_queue\n"
	const idleV4 = header + "  0: 00000000:480B 00000000:0000 0A 00000000:00000000\n" +
		"  1: 0100007F:480B 0100007F:DD45 06 00000000:00000000\n" +
		"  2: 0100007F:6F1B 0100007F:DD45 01 00000000:00000000\n"
	const activeV6 = header + "  0: 00000000000000000000000000000000:46A0 00000000000000000000000000000001:DD45 03 00000000:00000000\n"
	script := strings.Replace(countEdgeWorkerSockets, "/proc/net/tcp /proc/net/tcp6", `"$3" "$4"`, 1)
	for _, tc := range []struct {
		name, tcp4, tcp6, want string
	}{
		{name: "listeners and TIME_WAIT are not streams", tcp4: idleV4, tcp6: header, want: "0"},
		{name: "includes pending IPv6 streams", tcp4: idleV4, tcp6: activeV6, want: "1"},
		{name: "missing family", tcp4: idleV4},
		{name: "truncated inventory", tcp4: idleV4, tcp6: header + "  0: invalid\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			ip4, ip6 := filepath.Join(dir, "tcp"), filepath.Join(dir, "tcp6")
			if err := os.WriteFile(ip4, []byte(tc.tcp4), 0600); err != nil {
				t.Fatal(err)
			}
			if tc.tcp6 != "" {
				if err := os.WriteFile(ip6, []byte(tc.tcp6), 0600); err != nil {
					t.Fatal(err)
				}
			}
			output, err := exec.Command("sh", "-ceu", script, "sh", "46A0", "480B", ip4, ip6).Output()
			if tc.want == "" {
				if err == nil {
					t.Fatal("incomplete socket table passed")
				}
			} else if err != nil || strings.TrimSpace(string(output)) != tc.want {
				t.Fatalf("socket inventory=%q, err=%v, want %q", output, err, tc.want)
			}
		})
	}
}
