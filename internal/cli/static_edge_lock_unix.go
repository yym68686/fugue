//go:build !windows

package cli

import (
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
)

func acquireStaticEdgeCutoverLock(zone string) (func(), error) {
	p := filepath.Join(staticEdgeConfigDir(), "cutovers", zone+".lock")
	if e := os.MkdirAll(filepath.Dir(p), 0700); e != nil {
		return nil, e
	}
	f, e := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	if e = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); e != nil {
		f.Close()
		return nil, fmt.Errorf("cutover for %s already running: %w", zone, e)
	}
	return func() { _ = unix.Flock(int(f.Fd()), unix.LOCK_UN); _ = f.Close() }, nil
}
