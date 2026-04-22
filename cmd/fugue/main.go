package main

import (
	"fmt"
	"os"

	"fugue/internal/cli"
)

func main() {
	if err := cli.Run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(cli.ExitCodeForError(err))
	}
}
