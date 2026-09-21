package diagnosticprobe

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

// Discard excess bytes while continuing to drain the pipe; neither the child
// nor its output can keep a canceled session alive or grow memory unboundedly.
type boundedOutput struct {
	data      []byte
	limit     int
	truncated bool
}

func (b *boundedOutput) Write(p []byte) (int, error) {
	n := len(p)
	keep := min(n, b.limit-len(b.data))
	b.data = append(b.data, p[:keep]...)
	b.truncated = b.truncated || keep < n
	return n, nil
}

func diagnosticCommand(ctx context.Context, limit int, name string, args ...string) ([]byte, bool, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	configureCommandCancellation(cmd)
	cmd.WaitDelay = time.Second
	out := &boundedOutput{limit: limit}
	errout := &boundedOutput{limit: 4096}
	cmd.Stdout, cmd.Stderr = out, errout
	err := cmd.Run()
	if err != nil {
		if detail := safeText(strings.TrimSpace(string(errout.data))); detail != "" {
			err = fmt.Errorf("%w: %s", err, detail)
		}
	}
	return out.data, out.truncated, err
}
