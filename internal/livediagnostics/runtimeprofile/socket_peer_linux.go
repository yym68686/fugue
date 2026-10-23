//go:build linux

package runtimeprofile

import (
	"golang.org/x/sys/unix"
	"net"
	"os"
)

// Diagnostic Jobs deliberately drop DAC_OVERRIDE. A non-root provider must
// authorize the kernel-reported peer rather than require the observer to gain
// a broad filesystem bypass capability. No network listener is opened.
func runtimeSocketMode() os.FileMode {
	if os.Geteuid() != 0 {
		return 0666
	}
	return 0600
}

func runtimePeerAllowed(conn net.Conn, owner uint32) bool {
	socket, ok := conn.(*net.UnixConn)
	if !ok {
		return false
	}
	raw, err := socket.SyscallConn()
	if err != nil {
		return false
	}
	var peer *unix.Ucred
	err = raw.Control(func(fd uintptr) {
		var e error
		peer, e = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
		if e != nil {
			peer = nil
		}
	})
	return err == nil && peer != nil && runtimeUIDAllowed(peer.Uid, owner)
}

func runtimeUIDAllowed(uid, owner uint32) bool { return uid == 0 || uid == owner }
