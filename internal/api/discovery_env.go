package api

import (
	_ "embed"
	"strings"
)

//go:embed discovery_env.py
var discoveryEnvPython string

// Both installers verify and decode the same signed bundle. The fields each
// installer emits and its behavior without Python remain local to the caller.
var discoveryEnvPythonPrelude = strings.TrimSuffix(discoveryEnvPython, "\n")
