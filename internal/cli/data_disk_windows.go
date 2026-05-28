//go:build windows

package cli

func pullAvailableDiskBytes(root string) (uint64, bool) {
	return 0, false
}
