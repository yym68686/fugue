package diagnosticprobe

import (
	"net"

	"golang.org/x/sys/unix"
)

func runtimeSocketPeerPID(conn net.Conn) (int, error) {
	raw, err := conn.(*net.UnixConn).SyscallConn()
	if err != nil {
		return 0, err
	}
	var peer *unix.Ucred
	var peerErr error
	if err := raw.Control(func(fd uintptr) { peer, peerErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED) }); err != nil {
		return 0, err
	}
	if peerErr != nil {
		return 0, peerErr
	}
	return int(peer.Pid), nil
}
