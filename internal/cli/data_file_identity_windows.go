//go:build windows

package cli

import "os"

func dataFileIdentity(info os.FileInfo) dataLocalFileIdentity {
	return dataLocalFileIdentity{}
}
