//go:build darwin

package main

import "golang.org/x/sys/unix"

func statChangeTimes(stat unix.Stat_t) (int64, int64, int64, int64) {
	return stat.Mtim.Sec, stat.Mtim.Nsec, stat.Ctim.Sec, stat.Ctim.Nsec
}
