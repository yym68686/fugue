package proxyproto

import (
	"fmt"
	"net"
)

const unknownV1Header = "PROXY UNKNOWN\r\n"

func HeaderV1(source, destination net.Addr) string {
	src, ok := source.(*net.TCPAddr)
	if !ok || src == nil || src.IP == nil {
		return unknownV1Header
	}
	dst, ok := destination.(*net.TCPAddr)
	if !ok || dst == nil || dst.IP == nil {
		return unknownV1Header
	}

	src4 := src.IP.To4()
	dst4 := dst.IP.To4()
	if src4 != nil && dst4 != nil {
		return fmt.Sprintf("PROXY TCP4 %s %s %d %d\r\n", src4.String(), dst4.String(), src.Port, dst.Port)
	}
	if src.IP.To16() != nil && dst.IP.To16() != nil && src4 == nil && dst4 == nil {
		return fmt.Sprintf("PROXY TCP6 %s %s %d %d\r\n", src.IP.String(), dst.IP.String(), src.Port, dst.Port)
	}
	return unknownV1Header
}
