package cli

import "errors"

func acquireStaticEdgeCutoverLock(string) (func(), error) {
	return nil, errors.New("DNS cutover currently requires macOS or Linux for a crash-safe lock")
}
