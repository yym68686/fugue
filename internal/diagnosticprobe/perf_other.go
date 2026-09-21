//go:build !linux

package diagnosticprobe

func perfPreflight() map[string]any {
	return map[string]any{"error": "perf syscall is only available on Linux"}
}
