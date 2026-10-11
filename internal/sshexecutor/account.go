package sshexecutor

import (
	"bufio"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"strings"

	"fugue/internal/managedssh"
)

func accountLoginShell(account *user.User) (string, error) {
	file, err := os.Open("/etc/passwd")
	if err != nil {
		return "", managedssh.ErrCapabilities
	}
	defer file.Close()
	return readAccountLoginShell(file, account)
}

func readAccountLoginShell(reader io.Reader, account *user.User) (string, error) {
	bounded := &io.LimitedReader{R: reader, N: 2 << 20}
	scanner := bufio.NewScanner(bounded)
	selected := ""
	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), ":")
		if fields[0] != account.Username {
			continue
		}
		if len(fields) != 7 || fields[2] != account.Uid || fields[5] != account.HomeDir || selected != "" {
			return "", managedssh.ErrCapabilities
		}
		selected = fields[6]
		if selected == "" {
			selected = "/bin/sh"
		}
		if !filepath.IsAbs(selected) || filepath.Clean(selected) != selected || strings.ContainsAny(selected, "\x00\r\n") {
			return "", managedssh.ErrCapabilities
		}
	}
	if scanner.Err() != nil || bounded.N == 0 || selected == "" {
		return "", managedssh.ErrCapabilities
	}
	return selected, nil
}
