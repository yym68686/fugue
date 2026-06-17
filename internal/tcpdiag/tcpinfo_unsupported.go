//go:build !linux

package tcpdiag

import "net"

func SnapshotFromConn(conn net.Conn) Snapshot {
	return Snapshot{Error: "tcp_info is only available on linux"}
}
