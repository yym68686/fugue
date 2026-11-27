//go:build !linux

package main

import (
	"context"
	"errors"
	"net/netip"
)

func readNodeWorkerTranslation(context.Context, workerSocketTuple, netip.AddrPort) (netip.AddrPort, error) {
	return netip.AddrPort{}, errors.New("held socket conntrack observation requires the Linux network namespace that originated it")
}
