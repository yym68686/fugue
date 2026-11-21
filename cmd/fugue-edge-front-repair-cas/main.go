// Command fugue-edge-front-repair-cas performs only one short, reviewed
// repair operation against a retained Front's existing activation volume.
// It cannot perform an ordinary activation and has no network clients.
package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"fugue/internal/frontrepairstate"
	"io"
	"os"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, stdout io.Writer) error {
	if stdout == nil {
		return errors.New("stdout is nil")
	}
	flags := flag.NewFlagSet("fugue-edge-front-repair-cas", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	path := flags.String("state-file", "", "absolute group-local activation state file")
	repair := flags.String("repair-request", "", "short-lived journaled repair write")
	prepare := flags.String("repair-prepare-request", "", "short-lived journaled inactive Worker preparation")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || flags.NFlag() != 2 || *path == "" {
		return errors.New("Front repair requires state-file and exactly one repair command")
	}
	if *prepare != "" {
		return runRepair(*path, *prepare, true, stdout)
	}
	return runRepair(*path, *repair, false, stdout)
}

func runRepair(path, encoded string, prepare bool, stdout io.Writer) error {
	if len(encoded) == 0 || len(encoded) > 16<<10 {
		return errors.New("Front repair request size is invalid")
	}
	for _, c := range encoded {
		if !(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
			return errors.New("Front repair request encoding is invalid")
		}
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(encoded)
	if err != nil {
		return errors.New("Front repair request encoding is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var write frontrepairstate.ActivationRepairWrite
	if decoder.Decode(&write) != nil || decoder.Decode(&struct{}{}) != io.EOF || write.Schema != frontrepairstate.ActivationRepairWriteSchemaV1 {
		return errors.New("Front repair request schema is invalid")
	}
	if prepare {
		receipt, err := frontrepairstate.PrepareActivationRepairPublication(path, write.Request, write.ExpiresAt)
		if err != nil {
			return err
		}
		return writeReceipt(stdout, receipt)
	}
	receipt, err := frontrepairstate.ApplyActivationRepairCAS(path, write.Request, write.ExpiresAt)
	if err != nil {
		return err
	}
	return writeReceipt(stdout, receipt)
}

func writeReceipt(stdout io.Writer, receipt any) error {
	encoder := json.NewEncoder(stdout)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(receipt)
}
