//go:build linux

package diagnosticprobe

import (
	"unsafe"

	"golang.org/x/sys/unix"
)

// Open only a disabled event for the observer itself. This separates the
// kernel's syscall admission from any userspace perf tool permission checks.
func perfPreflight() map[string]any {
	result := map[string]any{"observer_security": processSecurity("/proc/self")}
	for _, path := range []string{"/proc/sys/kernel/perf_event_paranoid", "/proc/sys/kernel/kptr_restrict", "/proc/version"} {
		if value, err := readBounded(path, 4096); err == nil {
			result[path] = value
		}
	}
	attr := unix.PerfEventAttr{Type: unix.PERF_TYPE_SOFTWARE, Config: unix.PERF_COUNT_SW_CPU_CLOCK, Bits: unix.PerfBitDisabled}
	attr.Size = uint32(unsafe.Sizeof(attr))
	fd, err := unix.PerfEventOpen(&attr, 0, -1, -1, unix.PERF_FLAG_FD_CLOEXEC)
	if err != nil {
		result["self_cpu_clock_error"] = err.Error()
	} else {
		unix.Close(fd)
		result["self_cpu_clock_opened"] = true
	}
	return result
}
