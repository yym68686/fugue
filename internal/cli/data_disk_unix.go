//go:build darwin || linux

package cli

import "syscall"

func pullAvailableDiskBytes(root string) (uint64, bool) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(root, &stat); err != nil {
		return 0, false
	}
	return stat.Bavail * uint64(stat.Bsize), true
}
