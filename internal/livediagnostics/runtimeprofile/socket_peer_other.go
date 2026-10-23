//go:build !linux

package runtimeprofile

import (
	"net"
	"os"
)

// Other platforms retain the owner-only filesystem boundary.
func runtimeSocketMode() os.FileMode               { return 0600 }
func runtimePeerAllowed(_ net.Conn, _ uint32) bool { return true }
