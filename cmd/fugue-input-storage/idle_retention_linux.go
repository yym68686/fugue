//go:build linux

package main

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

const idleRetentionChild = "FUGUE_RETENTION_IDLE_CHILD"

// enterIdleRetention reexecutes only this executable, on a locked OS thread
// demoted before exec. All new Go threads therefore inherit SCHED_IDLE/nice19;
// changing only the calling thread of an existing Go runtime is insufficient.
// The environment marker is not authority: the child verifies every live
// thread and both runtime settings even when the caller supplies the marker.
func enterIdleRetention() error {
	if os.Getenv(idleRetentionChild) == "1" {
		return verifyIdleRetention()
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if err := unix.Setpriority(unix.PRIO_PROCESS, 0, 19); err != nil {
		return fmt.Errorf("idle priority: %w", err)
	}
	if err := unix.SchedSetAttr(0, &unix.SchedAttr{Policy: unix.SCHED_IDLE, Nice: 19}, 0); err != nil {
		return fmt.Errorf("idle policy: %w", err)
	}
	attr, err := unix.SchedGetAttr(0, 0)
	if err != nil || attr.Policy != unix.SCHED_IDLE || attr.Nice != 19 {
		return errors.New("idle launcher verification failed")
	}
	env := make([]string, 0, len(os.Environ())+3)
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if key != idleRetentionChild && key != "GOMAXPROCS" && key != "GOMEMLIMIT" {
			env = append(env, entry)
		}
	}
	env = append(env, idleRetentionChild+"=1", "GOMAXPROCS=1", "GOMEMLIMIT=128MiB")
	return unix.Exec("/proc/self/exe", os.Args, env)
}

func verifyIdleRetention() error {
	if runtime.GOMAXPROCS(0) != 1 || debug.SetMemoryLimit(-1) != 128<<20 {
		return errors.New("idle runtime settings unavailable")
	}
	threads, err := os.ReadDir("/proc/self/task")
	if err != nil || len(threads) == 0 || len(threads) > 128 {
		return errors.New("idle thread inventory unavailable")
	}
	verified := 0
	for _, thread := range threads {
		tid, parseErr := strconv.Atoi(thread.Name())
		if parseErr != nil || tid <= 0 {
			return errors.New("idle thread identity unavailable")
		}
		attr, attrErr := unix.SchedGetAttr(tid, 0)
		if errors.Is(attrErr, unix.ESRCH) {
			continue // A terminated thread cannot execute diagnostic work.
		}
		if attrErr != nil || attr.Policy != unix.SCHED_IDLE || attr.Nice != 19 {
			return errors.New("idle thread policy unavailable")
		}
		verified++
	}
	if verified == 0 {
		return errors.New("idle threads unavailable")
	}
	return nil
}
