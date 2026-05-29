//go:build darwin || linux

package cli

import (
	"os"
	"syscall"
)

func dataFileIdentity(info os.FileInfo) dataLocalFileIdentity {
	if info == nil {
		return dataLocalFileIdentity{}
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat == nil {
		return dataLocalFileIdentity{}
	}
	return dataLocalFileIdentity{
		Device: uint64(stat.Dev),
		Inode:  uint64(stat.Ino),
	}
}
