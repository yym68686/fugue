//go:build !linux && !darwin

package diagnosticprobe

import (
	"errors"
	"net"
)

func runtimeSocketPeerPID(net.Conn) (int, error) {
	return 0, errors.New("runtime socket peer identity is unavailable on this platform")
}
