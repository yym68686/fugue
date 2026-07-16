//go:build !darwin && !linux

package main

import "golang.org/x/sys/unix"

func statChangeTimes(unix.Stat_t) (int64, int64, int64, int64) {
	return 0, 0, 0, 0
}
