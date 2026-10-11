package sshexecutor

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"sync"

	"fugue/internal/managedssh"
)

const (
	frameStart       byte = 1
	frameInput       byte = 2
	frameEOF         byte = 3
	frameResize      byte = 4
	frameSignal      byte = 5
	frameOutput      byte = 6
	frameErrorOutput byte = 7
	frameExit        byte = 8
	frameFailure     byte = 9
	frameAck         byte = 10
	frameReady       byte = 11
	maxFrameSize          = 65536
)

type Invocation struct {
	Schema      string               `json:"schema"`
	Target      managedssh.Target    `json:"target"`
	Shell       string               `json:"shell"`
	Kind        string               `json:"kind"`
	Command     string               `json:"command,omitempty"`
	Terminal    *managedssh.Terminal `json:"terminal,omitempty"`
	Environment map[string]string    `json:"environment,omitempty"`
	ForwardHost string               `json:"forward_host,omitempty"`
	ForwardPort uint32               `json:"forward_port,omitempty"`
}

type frameWriter struct {
	writer io.Writer
	mutex  sync.Mutex
}

func (writer *frameWriter) send(kind byte, payload []byte) error {
	if len(payload) > maxFrameSize {
		return errors.New("managed SSH frame too large")
	}
	writer.mutex.Lock()
	defer writer.mutex.Unlock()
	var header [5]byte
	header[0] = kind
	binary.BigEndian.PutUint32(header[1:], uint32(len(payload)))
	if _, err := writer.writer.Write(header[:]); err != nil {
		return err
	}
	_, err := writer.writer.Write(payload)
	return err
}

func readFrame(reader io.Reader) (byte, []byte, error) {
	var header [5]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return 0, nil, err
	}
	size := binary.BigEndian.Uint32(header[1:])
	if size > maxFrameSize {
		return 0, nil, errors.New("managed SSH frame too large")
	}
	payload := make([]byte, int(size))
	_, err := io.ReadFull(reader, payload)
	return header[0], payload, err
}

type outputFrames struct {
	writer *frameWriter
	kind   byte
}

func (output outputFrames) Write(payload []byte) (int, error) {
	written := 0
	for len(payload) > 0 {
		size := len(payload)
		if size > 32768 {
			size = 32768
		}
		if err := output.writer.send(output.kind, payload[:size]); err != nil {
			return written, err
		}
		written += size
		payload = payload[size:]
	}
	return written, nil
}

func ServeHelper(ctx context.Context, stream io.ReadWriteCloser) error {
	defer stream.Close()
	kind, payload, err := readFrame(stream)
	if err != nil {
		return err
	}
	if kind != frameStart {
		return errors.New("managed SSH helper requires invocation")
	}
	var invocation Invocation
	if err := json.Unmarshal(payload, &invocation); err != nil || invocation.Schema != "fugue.ssh-execution/v1" {
		return managedssh.ErrConfiguration
	}
	writer := &frameWriter{writer: stream}
	process, err := NewProcess(invocation.Target, invocation.Shell)
	if err != nil {
		_ = writer.send(frameFailure, []byte("runtime capability or user mismatch"))
		return err
	}
	switch invocation.Kind {
	case "exec", "shell", "sftp", "forward":
	default:
		return managedssh.ErrCapabilities
	}
	if invocation.Terminal != nil && (invocation.Terminal.Columns == 0 || invocation.Terminal.Columns > 65535 || invocation.Terminal.Rows == 0 || invocation.Terminal.Rows > 65535) {
		return managedssh.ErrConfiguration
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(ctx, func() { _ = stream.Close() })
	defer stop()
	input, inputWriter := io.Pipe()
	defer input.Close()
	defer inputWriter.Close()
	resize := make(chan managedssh.Resize, 16)
	signals := make(chan string, 16)
	data := make(chan []byte, 1)
	go func() {
		defer inputWriter.Close()
		for {
			select {
			case <-ctx.Done():
				return
			case payload, open := <-data:
				if !open {
					return
				}
				if _, err := inputWriter.Write(payload); err != nil {
					return
				}
				if err := writer.send(frameAck, nil); err != nil {
					cancel()
					return
				}
			}
		}
	}()
	go func() {
		defer cancel()
		eof := false
		for {
			kind, payload, err := readFrame(stream)
			if err != nil {
				return
			}
			switch kind {
			case frameInput:
				if eof {
					return
				}
				select {
				case data <- payload:
				default:
					return
				}
			case frameEOF:
				if eof || len(payload) != 0 {
					return
				}
				eof = true
				close(data)
			case frameResize:
				var size managedssh.Resize
				if json.Unmarshal(payload, &size) != nil {
					return
				}
				select {
				case resize <- size:
				default:
					return
				}
			case frameSignal:
				if _, ok := processSignal(string(payload)); !ok {
					return
				}
				select {
				case signals <- string(payload):
				default:
					return
				}
			default:
				return
			}
		}
	}()
	if err := writer.send(frameReady, nil); err != nil {
		return err
	}
	request := managedssh.SessionRequest{Kind: invocation.Kind, Command: invocation.Command, Terminal: invocation.Terminal, Environment: invocation.Environment, Resize: resize, Signals: signals}
	var exit managedssh.Exit
	if invocation.Kind == "forward" {
		connection, forwardErr := process.Forward(ctx, invocation.Target, invocation.ForwardHost, invocation.ForwardPort)
		if forwardErr != nil {
			_ = writer.send(frameFailure, []byte("forward target unavailable"))
			return forwardErr
		}
		defer connection.Close()
		stopForward := context.AfterFunc(ctx, func() { _ = connection.Close() })
		defer stopForward()
		go func() {
			_, _ = io.Copy(connection, input)
			if half, ok := connection.(interface{ CloseWrite() error }); ok {
				_ = half.CloseWrite()
			} else {
				_ = connection.Close()
			}
		}()
		_, err = io.Copy(outputFrames{writer: writer, kind: frameOutput}, connection)
	} else {
		exit, err = process.Run(ctx, invocation.Target, request, managedssh.SessionIO{Stdin: input, Stdout: outputFrames{writer: writer, kind: frameOutput}, Stderr: outputFrames{writer: writer, kind: frameErrorOutput}})
	}
	if err != nil {
		_ = writer.send(frameFailure, []byte("managed runtime execution failed"))
		return err
	}
	payload, err = json.Marshal(exit)
	if err != nil {
		return err
	}
	return writer.send(frameExit, payload)
}

func ExecuteHelper(ctx context.Context, stream io.ReadWriteCloser, invocation Invocation, request managedssh.SessionRequest, streams managedssh.SessionIO) (managedssh.Exit, error) {
	defer stream.Close()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(ctx, func() { _ = stream.Close() })
	defer stop()
	writer := &frameWriter{writer: stream}
	payload, err := json.Marshal(invocation)
	if err != nil {
		return managedssh.Exit{}, err
	}
	if err := writer.send(frameStart, payload); err != nil {
		return managedssh.Exit{}, err
	}
	kind, payload, err := readFrame(stream)
	if err != nil {
		return managedssh.Exit{}, err
	}
	if kind != frameReady || len(payload) != 0 {
		return managedssh.Exit{}, managedssh.ErrCapabilities
	}
	acknowledged := make(chan struct{}, 1)
	go func() {
		buffer := make([]byte, 32768)
		for {
			count, err := streams.Stdin.Read(buffer)
			if count > 0 {
				if writer.send(frameInput, buffer[:count]) != nil {
					cancel()
					return
				}
				select {
				case <-acknowledged:
				case <-ctx.Done():
					return
				}
			}
			if err != nil {
				if errors.Is(err, io.EOF) {
					_ = writer.send(frameEOF, nil)
				} else {
					cancel()
				}
				return
			}
		}
	}()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case size, open := <-request.Resize:
				if !open {
					return
				}
				payload, err := json.Marshal(size)
				if err != nil || writer.send(frameResize, payload) != nil {
					cancel()
					return
				}
			case signal, open := <-request.Signals:
				if !open {
					return
				}
				if writer.send(frameSignal, []byte(signal)) != nil {
					cancel()
					return
				}
			}
		}
	}()
	for {
		kind, payload, err := readFrame(stream)
		if err != nil {
			return managedssh.Exit{}, err
		}
		switch kind {
		case frameOutput:
			if _, err := streams.Stdout.Write(payload); err != nil {
				return managedssh.Exit{}, err
			}
		case frameErrorOutput:
			if _, err := streams.Stderr.Write(payload); err != nil {
				return managedssh.Exit{}, err
			}
		case frameAck:
			if len(payload) != 0 {
				return managedssh.Exit{}, managedssh.ErrConfiguration
			}
			select {
			case acknowledged <- struct{}{}:
			default:
				return managedssh.Exit{}, managedssh.ErrConfiguration
			}
		case frameExit:
			var exit managedssh.Exit
			if err := json.Unmarshal(payload, &exit); err != nil {
				return managedssh.Exit{}, err
			}
			return exit, nil
		case frameFailure:
			return managedssh.Exit{}, errors.New("managed SSH helper reported execution failure")
		default:
			return managedssh.Exit{}, managedssh.ErrConfiguration
		}
	}
}
