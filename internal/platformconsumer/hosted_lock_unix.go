//go:build linux || darwin

package platformconsumer

import (
	"os"
	"syscall"
)

// Closing this descriptor releases its lock, even on process death.
func lockHostedCheckpoint(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}
