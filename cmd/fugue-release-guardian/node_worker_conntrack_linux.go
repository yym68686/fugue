package main

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"net/netip"
	"time"

	"golang.org/x/sys/unix"
)

func readNodeWorkerTranslation(ctx context.Context, tuple workerSocketTuple, front netip.AddrPort) (netip.AddrPort, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var random [4]byte
	if _, err := rand.Read(random[:]); err != nil {
		return netip.AddrPort{}, err
	}
	sequence := binary.NativeEndian.Uint32(random[:]) | 1
	wire, err := workerCTQuery(tuple, sequence)
	if err != nil {
		return netip.AddrPort{}, err
	}
	fd, err := unix.Socket(unix.AF_NETLINK, unix.SOCK_RAW|unix.SOCK_CLOEXEC|unix.SOCK_NONBLOCK, unix.NETLINK_NETFILTER)
	if err != nil {
		return netip.AddrPort{}, err
	}
	defer unix.Close(fd)
	kernel := &unix.SockaddrNetlink{Family: unix.AF_NETLINK}
	if err := unix.Bind(fd, kernel); err != nil {
		return netip.AddrPort{}, err
	}
	if err := unix.Sendto(fd, wire, 0, kernel); err != nil {
		return netip.AddrPort{}, err
	}
	raw := make([]byte, 65536)
	for ctx.Err() == nil {
		poll := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
		n, err := unix.Poll(poll, 100)
		if err == unix.EINTR || err == nil && n == 0 {
			continue
		}
		if err != nil || poll[0].Revents&unix.POLLIN == 0 {
			return netip.AddrPort{}, errors.Join(errors.New("held socket conntrack read unavailable"), err)
		}
		n, _, flags, sender, err := unix.Recvmsg(fd, raw, nil, 0)
		if err == unix.EAGAIN || err == unix.EINTR {
			continue
		}
		from, ok := sender.(*unix.SockaddrNetlink)
		if err != nil || !ok || from.Pid != 0 || from.Groups != 0 || flags&unix.MSG_TRUNC != 0 {
			return netip.AddrPort{}, errors.Join(errors.New("held socket conntrack response is truncated or not from the kernel"), err)
		}
		return workerCTTranslation(raw[:n], sequence, tuple, front)
	}
	return netip.AddrPort{}, ctx.Err()
}
