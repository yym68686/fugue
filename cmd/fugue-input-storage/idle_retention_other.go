//go:build !linux

package main

import "errors"

func enterIdleRetention() error {
	return errors.New("idle retention requires Linux scheduling verification")
}
