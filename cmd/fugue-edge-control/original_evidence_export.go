package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"strings"

	"fugue/internal/edgecontrol"
)

// This command runs before production configuration is loaded. The caller
// must capture stdout privately; no serving store or filesystem is written.
func runOriginalEvidenceExport(ctx context.Context, args []string, output io.Writer) error {
	flags := flag.NewFlagSet("export-group-original-evidence", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	state := flags.String("state-file", "", "exact original persistent state file")
	group := flags.String("edge-group-id", "", "exact group identity")
	keyring := flags.String("signing-keyring-dir", "", "independently selected current group keyring directory")
	selection := flags.String("generations", "", "1-32 comma-separated exact generations")
	if flags.Parse(args) != nil || flags.NArg() != 0 || len(*selection) == 0 || len(*selection) > 32*513 {
		return errors.New("invalid export-group-original-evidence arguments")
	}
	generations := strings.Split(*selection, ",")
	report, err := edgecontrol.ExportGroupOriginalEvidence(ctx, *state, *group, *keyring, generations)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(report)
	if err != nil || len(encoded) > 32<<20 {
		return errors.New("export-group-original-evidence report exceeds its bound")
	}
	n, err := output.Write(encoded)
	if err == nil && n != len(encoded) {
		return io.ErrShortWrite
	}
	return err
}
