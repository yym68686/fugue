package operationexecution

import (
	"bytes"
	"errors"
	"log"
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestOperationTimingDistinguishesTrafficPendingFromFailure(t *testing.T) {
	for _, tc := range []struct {
		err     error
		pending bool
		status  string
	}{{nil, false, "completed"}, {errors.New("traffic publication is pending"), true, "pending"}, {errors.New("executable failed"), false, "failed"}} {
		var output bytes.Buffer
		timer := NewTimer(nil)
		timer.Mark("traffic_publication_wait")
		timer.Log(log.New(&output, "", 0), "managed_operation", model.Operation{ID: "operation-a"}, tc.err, tc.pending)
		if !strings.Contains(output.String(), "status="+tc.status) || !strings.Contains(output.String(), "traffic_publication_wait=") {
			t.Fatal("misleading stage timing", output.String())
		}
	}
}
