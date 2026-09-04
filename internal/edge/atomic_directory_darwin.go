//go:build darwin

package edge

import "golang.org/x/sys/unix"

func exchangeDirectories(staged, current string) error {
	return unix.RenamexNp(staged, current, unix.RENAME_SWAP)
}
