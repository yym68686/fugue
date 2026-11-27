package operationread

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

type failingTimingQuery struct{ err error }

func (q failingTimingQuery) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, q.err
}

func TestFirstDeployExecutionStartsShortCircuitsAndRetainsReadErrors(t *testing.T) {
	if got, err := FirstDeployExecutionStarts(t.Context(), nil, nil); err != nil || len(got) != 0 {
		t.Fatalf("zero input got %v, err %v", got, err)
	}
	readErr := errors.New("unavailable")
	if _, err := FirstDeployExecutionStarts(t.Context(), failingTimingQuery{readErr}, []string{"deploy"}); !errors.Is(err, readErr) {
		t.Fatalf("read error was replaced: %v", err)
	}
}
