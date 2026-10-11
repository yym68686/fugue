package sshexecutor

import (
	"os"

	"golang.org/x/crypto/ssh"
	"golang.org/x/sys/unix"
)

func setTerminalModes(terminal *os.File, modes map[uint8]uint32) error {
	settings, err := unix.IoctlGetTermios(int(terminal.Fd()), unix.TIOCGETA)
	if err != nil {
		return err
	}
	for opcode, value := range modes {
		var field *uint64
		var mask uint64
		switch opcode {
		case ssh.ECHO:
			field = &settings.Lflag
			mask = unix.ECHO
		case ssh.ICANON:
			field = &settings.Lflag
			mask = unix.ICANON
		case ssh.ISIG:
			field = &settings.Lflag
			mask = unix.ISIG
		case ssh.IEXTEN:
			field = &settings.Lflag
			mask = unix.IEXTEN
		case ssh.ICRNL:
			field = &settings.Iflag
			mask = unix.ICRNL
		case ssh.IXON:
			field = &settings.Iflag
			mask = unix.IXON
		case ssh.OPOST:
			field = &settings.Oflag
			mask = unix.OPOST
		case ssh.ONLCR:
			field = &settings.Oflag
			mask = unix.ONLCR
		case ssh.VINTR:
			settings.Cc[unix.VINTR] = uint8(value)
		case ssh.VEOF:
			settings.Cc[unix.VEOF] = uint8(value)
		case ssh.VERASE:
			settings.Cc[unix.VERASE] = uint8(value)
		}
		if field != nil {
			if value != 0 {
				*field |= mask
			} else {
				*field &^= mask
			}
		}
	}
	return unix.IoctlSetTermios(int(terminal.Fd()), unix.TIOCSETA, settings)
}
