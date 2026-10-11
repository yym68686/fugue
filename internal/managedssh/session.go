package managedssh

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"time"

	"fugue/internal/model"

	"golang.org/x/crypto/ssh"
)

func (server *Server) handleSession(ctx context.Context, connectionState, policy *snapshot, incoming ssh.NewChannel, connectionID, fingerprint string) {
	if len(incoming.ExtraData()) != 0 {
		_ = incoming.Reject(ssh.Prohibited, "invalid session data")
		return
	}
	channel, requests, err := incoming.Accept()
	if err != nil {
		return
	}
	defer channel.Close()
	sessionContext, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(sessionContext, func() { _ = channel.Close() })
	defer stop()
	resize := make(chan Resize, 16)
	signals := make(chan string, 16)
	request := SessionRequest{ID: model.NewID("sshsession"), Environment: map[string]string{}, Resize: resize, Signals: signals}
	terminalSize := Resize{Columns: 80, Rows: 24}
	started := false
	completed := make(chan Exit, 1)
	initialTimer := time.NewTimer(30 * time.Second)
	defer initialTimer.Stop()
	for {
		select {
		case <-sessionContext.Done():
			return
		case <-initialTimer.C:
			if !started {
				return
			}
		case exit := <-completed:
			if exit.Signal != "" {
				_, _ = channel.SendRequest("exit-signal", false, ssh.Marshal(struct {
					Signal            string
					CoreDumped        bool
					Message, Language string
				}{exit.Signal, false, "", ""}))
			}
			_, _ = channel.SendRequest("exit-status", false, ssh.Marshal(struct{ Status uint32 }{exit.Code}))
			_ = server.record(context.WithoutCancel(ctx), connectionState, "session_closed", connectionID, request.ID, fingerprint, "", exit.Code)
			return
		case message, open := <-requests:
			if !open {
				return
			}
			if len(message.Payload) > 65536 {
				_ = message.Reply(false, nil)
				continue
			}
			current := server.state.Load()
			if !current.authorize(connectionState.configuration.Target.User, fingerprint, time.Now().UTC()) {
				_ = server.record(context.WithoutCancel(sessionContext), current, "session_request_rejected", connectionID, request.ID, fingerprint, "authorization_expired_or_revoked", 0)
				_ = message.Reply(false, nil)
				return
			}
			accepted := false
			switch message.Type {
			case "env":
				var value struct{ Name, Value string }
				if !started && ssh.Unmarshal(message.Payload, &value) == nil && len(request.Environment) < 32 && len(value.Value) <= 4096 && !strings.ContainsRune(value.Value, 0) && environmentAllowed(current.configuration, value.Name) {
					request.Environment[value.Name] = value.Value
					accepted = true
				}
			case "pty-req":
				var value struct {
					Name                         string
					Columns, Rows, Width, Height uint32
					Modes                        string
				}
				if !started && request.Terminal == nil && current.configuration.AllowPTY && ssh.Unmarshal(message.Payload, &value) == nil && (value.Name == "" || safeToken(value.Name)) && validSize(value.Columns, value.Rows) {
					modes, err := terminalModes([]byte(value.Modes))
					if err == nil {
						terminalSize = retainTerminalDimensions(terminalSize, value.Columns, value.Rows)
						if value.Name == "" {
							value.Name = "dumb"
						}
						request.Terminal = &Terminal{Name: value.Name, Columns: terminalSize.Columns, Rows: terminalSize.Rows, Modes: modes}
						accepted = true
					}
				}
			case "window-change":
				var value struct{ Columns, Rows, Width, Height uint32 }
				if request.Terminal != nil && ssh.Unmarshal(message.Payload, &value) == nil && validSize(value.Columns, value.Rows) {
					nextSize := retainTerminalDimensions(terminalSize, value.Columns, value.Rows)
					select {
					case resize <- nextSize:
						terminalSize = nextSize
						accepted = true
					default:
					}
				}
			case "signal":
				var value struct{ Name string }
				if started && ssh.Unmarshal(message.Payload, &value) == nil && allowedSignal(value.Name) {
					select {
					case signals <- value.Name:
						accepted = true
					default:
					}
				}
			case "shell", "exec", "subsystem":
				if started || current.configuration.Target != connectionState.configuration.Target {
					_ = server.record(sessionContext, current, "session_request_rejected", connectionID, request.ID, fingerprint, "session_started_or_target_changed", 0)
					break
				}
				valid := false
				switch message.Type {
				case "shell":
					valid = len(message.Payload) == 0
					request.Kind = "shell"
				case "exec":
					var value struct{ Command string }
					valid = ssh.Unmarshal(message.Payload, &value) == nil && value.Command != "" && len(value.Command) <= 32768 && !strings.ContainsRune(value.Command, 0)
					request.Kind = "exec"
					request.Command = value.Command
				case "subsystem":
					var value struct{ Name string }
					valid = ssh.Unmarshal(message.Payload, &value) == nil && value.Name == "sftp" && current.configuration.AllowSFTP && request.Terminal == nil
					request.Kind = "sftp"
				}
				if !valid {
					_ = server.record(sessionContext, current, "session_request_rejected", connectionID, request.ID, fingerprint, "invalid_request", 0)
					break
				}
				capabilities, err := server.executor.Validate(sessionContext, connectionState.configuration.Target)
				if err != nil {
					_ = server.record(context.WithoutCancel(sessionContext), current, "session_request_rejected", connectionID, request.ID, fingerprint, RuntimeFailureReason(err), 0)
					break
				}
				if !sessionSupported(capabilities, request) {
					_ = server.record(sessionContext, current, "session_request_rejected", connectionID, request.ID, fingerprint, "capability_not_supported", 0)
					break
				}
				if err := server.record(sessionContext, current, "session_opened", connectionID, request.ID, fingerprint, request.Kind, 0); err != nil {
					break
				}
				started = true
				accepted = true
				initialTimer.Stop()
				frozen := request
				go func() {
					standardError := io.Writer(channel.Stderr())
					if frozen.Terminal != nil {
						standardError = channel
					}
					exit, err := server.executor.Run(sessionContext, connectionState.configuration.Target, frozen, SessionIO{Stdin: channel, Stdout: channel, Stderr: standardError})
					if err != nil {
						exit = Exit{Code: 255}
						_, _ = io.WriteString(channel.Stderr(), "managed SSH execution failed\n")
					}
					completed <- exit
				}()
			}
			_ = message.Reply(accepted, nil)
		}
	}
}

func sessionSupported(capabilities Capabilities, request SessionRequest) bool {
	if !capabilities.Signals || (request.Terminal != nil && !capabilities.PTY) {
		return false
	}
	switch request.Kind {
	case "shell":
		return capabilities.Shell
	case "exec":
		return capabilities.Exec
	case "sftp":
		return capabilities.SFTP
	default:
		return false
	}
}

func environmentAllowed(configuration Configuration, name string) bool {
	for _, allowed := range configuration.AllowedEnvironment {
		if name == allowed {
			return true
		}
	}
	return false
}

func validSize(columns, rows uint32) bool {
	return columns <= 65535 && rows <= 65535
}

func retainTerminalDimensions(previous Resize, columns, rows uint32) Resize {
	if columns != 0 {
		previous.Columns = columns
	}
	if rows != 0 {
		previous.Rows = rows
	}
	return previous
}

func allowedSignal(signal string) bool {
	switch signal {
	case "ABRT", "ALRM", "FPE", "HUP", "ILL", "INT", "KILL", "PIPE", "QUIT", "SEGV", "TERM", "USR1", "USR2":
		return true
	default:
		return false
	}
}

func terminalModes(encoded []byte) (map[uint8]uint32, error) {
	modes := map[uint8]uint32{}
	if len(encoded) > 1024 {
		return nil, ErrConfiguration
	}
	for len(encoded) > 0 {
		opcode := encoded[0]
		encoded = encoded[1:]
		if opcode == 0 {
			if len(encoded) != 0 {
				return nil, ErrConfiguration
			}
			return modes, nil
		}
		if opcode >= 160 {
			return modes, nil
		}
		if len(encoded) < 4 {
			return nil, fmt.Errorf("invalid terminal mode encoding")
		}
		modes[opcode] = binary.BigEndian.Uint32(encoded[:4])
		encoded = encoded[4:]
	}
	return nil, ErrConfiguration
}
