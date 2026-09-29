package staticedgeobserve

import (
	"errors"
	"os"
)

func lockEvidence(*os.File) error {
	return errors.New("local collector requires Linux or macOS; remote CLI works on Windows")
}
