//go:build linux

package runtimeprofile

import (
	"net"
	"os"
	"testing"
)

func TestRuntimeProfileRejectsOtherUsersAndNonUnixPeers(t *testing.T) {
	for _, row := range []struct {
		uid, owner uint32
		allowed    bool
	}{{0, 65532, true}, {65532, 65532, true}, {1000, 65532, false}, {65532, 0, false}, {0, 0, true}} {
		if runtimeUIDAllowed(row.uid, row.owner) != row.allowed {
			t.Fatalf("unexpected peer authorization: %+v", row)
		}
	}
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	if runtimePeerAllowed(a, uint32(os.Geteuid())) {
		t.Fatal("non-Unix peer bypassed kernel credential check")
	}
}

func TestRuntimeProfileChecksActualKernelPeer(t *testing.T) {
	socket, err := net.ListenUnix("unix", &net.UnixAddr{Name: t.TempDir() + "/profile.sock", Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	client, err := net.DialUnix("unix", nil, socket.Addr().(*net.UnixAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	server, err := socket.AcceptUnix()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	uid := uint32(os.Geteuid())
	if !runtimePeerAllowed(server, uid) {
		t.Fatal("kernel-authenticated owner was rejected")
	}
	if uid != 0 && runtimePeerAllowed(server, uid+1) {
		t.Fatal("another UID bypassed socket authentication")
	}
}
