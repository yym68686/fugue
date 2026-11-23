//go:build !linux && !darwin

package platformconsumer

import (
	"errors"
	"os"
)

// Non-executor CLI platforms may read selected content but must not
// silently replace a durable head without cross-process serialization.
func lockHostedCheckpoint(*os.File) error {
	return errors.New("host checkpoint locking unsupported on this platform")
}
