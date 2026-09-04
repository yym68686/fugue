//go:build !linux && !darwin

package edge

import "errors"

func exchangeDirectories(_, _ string) error {
	return errors.New("atomic directory exchange is unsupported on this platform")
}
