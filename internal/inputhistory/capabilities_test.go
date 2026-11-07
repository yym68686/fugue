package inputhistory

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"testing"
)

func TestCapabilityDiagnosticOwnsArgumentsAndDoesNotFallThrough(t *testing.T) {
	for _, args := range [][]string{{}, {"--mode", "observe"}} {
		var out bytes.Buffer
		if handled, err := WriteCapabilities(args, "store", &out); handled || err != nil || out.Len() != 0 {
			t.Fatal("ordinary command intercepted", handled, err)
		}
	}
	for _, args := range [][]string{{CapabilityArgument, "--apply"}, {"--mode", "archive-retire", CapabilityArgument}, {CapabilityArgument, CapabilityArgument}} {
		var out bytes.Buffer
		if handled, err := WriteCapabilities(args, "store", &out); !handled || err == nil || out.Len() != 0 {
			t.Fatal("malformed diagnostic may start a service or mutation", handled, err)
		}
	}
	for _, role := range []string{"store", "schema"} {
		var out bytes.Buffer
		if handled, err := WriteCapabilities([]string{CapabilityArgument}, role, &out); !handled || err != nil {
			t.Fatal(handled, err)
		}
		var report Report
		if json.Unmarshal(out.Bytes(), &report) != nil || report.Role != role || len(report.ContractSHA256) != 64 || len(report.Capabilities) == 0 {
			t.Fatal("invalid diagnostic", out.String())
		}
		// The caller cannot mutate the process's embedded protocol declaration.
		original := report.Capabilities[0]
		report.Capabilities[0] = "changed"
		fresh, _ := BinaryCapabilities(role)
		if fresh.Capabilities[0] != original {
			t.Fatal("capability response aliases the contract")
		}
		var fields map[string]json.RawMessage
		_ = json.Unmarshal(out.Bytes(), &fields)
		want := map[string]bool{"schema": true, "role": true, "capabilities": true, "contract_sha256": true}
		got := map[string]bool{}
		for key := range fields {
			got[key] = true
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatal("diagnostic exposed configuration or input state", got)
		}
	}
	if _, err := BinaryCapabilities("operator-says-ready"); err == nil {
		t.Fatal("unknown role accepted")
	}
	if handled, err := WriteCapabilities([]string{CapabilityArgument}, "store", failedWriter{}); !handled || !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal("failed output disguised as a complete report", handled, err)
	}
}

type failedWriter struct{}

func (failedWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }
