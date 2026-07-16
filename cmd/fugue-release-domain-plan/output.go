package main

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

const atomicOutputCreateAttempts = 128

type privateAtomicOutput struct {
	requestedParent string
	resolvedParent  string
	resolvedOutput  string
	base            string
	parent          *os.File
	parentStat      unix.Stat_t
	protectedInputs []protectedInputSnapshot
	file            *os.File
	fileStat        unix.Stat_t
	temporaryName   string
	destinationStat unix.Stat_t
	destinationSet  bool
	published       bool
}

type protectedInputSnapshot struct {
	requestedPath string
	resolvedPath  string
	fileStat      unix.Stat_t
}

func writePrivateAtomicFile(filename string, data []byte, protectedPaths ...string) error {
	output, err := createPrivateAtomicOutput(filename, protectedPaths...)
	if err != nil {
		return err
	}
	publishErr := output.publish(data)
	closeErr := output.close()
	return errors.Join(publishErr, closeErr)
}

func createPrivateAtomicOutput(filename string, protectedPaths ...string) (*privateAtomicOutput, error) {
	absolute, err := filepath.Abs(filename)
	if err != nil {
		return nil, fmt.Errorf("resolve output path: %w", err)
	}
	base := filepath.Base(absolute)
	if base == "." || base == ".." || base == string(filepath.Separator) {
		return nil, fmt.Errorf("output path must name one file")
	}
	requestedParent := filepath.Dir(absolute)
	resolvedParent, err := filepath.EvalSymlinks(requestedParent)
	if err != nil {
		return nil, fmt.Errorf("resolve output parent: %w", err)
	}

	parentFD, err := unix.Open(resolvedParent, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("securely open output parent: %w", err)
	}
	parent := os.NewFile(uintptr(parentFD), resolvedParent)
	if parent == nil {
		_ = unix.Close(parentFD)
		return nil, fmt.Errorf("adopt securely opened output parent")
	}
	output := &privateAtomicOutput{
		requestedParent: requestedParent,
		resolvedParent:  resolvedParent,
		resolvedOutput:  filepath.Join(resolvedParent, base),
		base:            base,
		parent:          parent,
	}
	fail := func(failure error) (*privateAtomicOutput, error) {
		return nil, errors.Join(failure, output.close())
	}

	if err := unix.Fstat(parentFD, &output.parentStat); err != nil {
		return fail(fmt.Errorf("inspect opened output parent: %w", err))
	}
	if err := validatePrivateParent(output.parentStat); err != nil {
		return fail(err)
	}
	if err := output.verifyParentIdentity(); err != nil {
		return fail(err)
	}
	protectedInputs, err := snapshotProtectedInputs(protectedPaths)
	if err != nil {
		return fail(err)
	}
	output.protectedInputs = protectedInputs

	destinationStat, exists, err := inspectOutputAt(parentFD, base)
	if err != nil {
		return fail(err)
	}
	if err := output.rejectProtectedInputAlias(destinationStat, exists); err != nil {
		return fail(err)
	}
	if exists {
		if err := validatePrivateFile(destinationStat, "existing output"); err != nil {
			return fail(err)
		}
		output.destinationStat = destinationStat
		output.destinationSet = true
	}
	// Re-resolve every protected input immediately before creating the
	// temporary output. This closes the window between taking the input
	// snapshots and opening a file that could later replace one of them.
	if err := output.verifyProtectedInputs(); err != nil {
		return fail(err)
	}

	for attempt := 0; attempt < atomicOutputCreateAttempts; attempt++ {
		temporaryName, err := randomAtomicOutputName()
		if err != nil {
			return fail(err)
		}
		fileFD, openErr := unix.Openat(parentFD, temporaryName, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
		if errors.Is(openErr, unix.EEXIST) {
			continue
		}
		if openErr != nil {
			return fail(fmt.Errorf("create fresh private output: %w", openErr))
		}
		file := os.NewFile(uintptr(fileFD), temporaryName)
		if file == nil {
			_ = unix.Close(fileFD)
			unlinkErr := unix.Unlinkat(parentFD, temporaryName, 0)
			return fail(errors.Join(fmt.Errorf("adopt fresh private output"), unlinkErr))
		}
		output.file = file
		output.temporaryName = temporaryName
		if err := unix.Fchmod(fileFD, 0o600); err != nil {
			return fail(fmt.Errorf("set fresh private output mode: %w", err))
		}
		if err := unix.Fstat(fileFD, &output.fileStat); err != nil {
			return fail(fmt.Errorf("inspect fresh private output: %w", err))
		}
		if err := validatePrivateFile(output.fileStat, "fresh output"); err != nil {
			return fail(err)
		}
		if err := output.verifyTemporaryIdentity(); err != nil {
			return fail(err)
		}
		return output, nil
	}
	return fail(fmt.Errorf("create fresh private output: exhausted %d collision attempts", atomicOutputCreateAttempts))
}

func (output *privateAtomicOutput) publish(data []byte) error {
	if output == nil || output.parent == nil || output.file == nil || output.temporaryName == "" || output.published {
		return fmt.Errorf("private output is not publishable")
	}
	if err := writeAll(output.file, data); err != nil {
		return fmt.Errorf("write fresh private output: %w", err)
	}
	if err := output.file.Sync(); err != nil {
		return fmt.Errorf("sync fresh private output: %w", err)
	}
	if err := output.verifyParentIdentity(); err != nil {
		return err
	}
	if err := output.verifyTemporaryIdentity(); err != nil {
		return err
	}
	if err := output.verifyDestinationUnchanged(); err != nil {
		return err
	}
	// This is deliberately the final validation before Renameat. In
	// particular, a protected path whose symlinked parent was retargeted to
	// the output must never be atomically replaced.
	if err := output.verifyProtectedInputs(); err != nil {
		return err
	}

	parentFD := int(output.parent.Fd())
	if err := unix.Renameat(parentFD, output.temporaryName, parentFD, output.base); err != nil {
		return fmt.Errorf("atomically publish private output: %w", err)
	}
	output.temporaryName = ""
	output.published = true
	if err := output.verifyPublishedIdentity(); err != nil {
		return err
	}
	if err := output.parent.Sync(); err != nil {
		return fmt.Errorf("sync output parent: %w", err)
	}
	if err := output.verifyParentIdentity(); err != nil {
		return err
	}
	if err := output.verifyPublishedIdentity(); err != nil {
		return err
	}
	return nil
}

func snapshotProtectedInputs(paths []string) ([]protectedInputSnapshot, error) {
	inputs := make([]protectedInputSnapshot, 0, len(paths))
	for _, path := range paths {
		absolute, err := filepath.Abs(path)
		if err != nil {
			return nil, fmt.Errorf("resolve protected input path: %w", err)
		}
		resolved, err := filepath.EvalSymlinks(absolute)
		if err != nil {
			return nil, fmt.Errorf("resolve protected input path: %w", err)
		}
		requestedStat, err := statFollowingSymlinks(absolute)
		if err != nil {
			return nil, fmt.Errorf("inspect protected input path: %w", err)
		}
		resolvedStat, err := statFollowingSymlinks(resolved)
		if err != nil {
			return nil, fmt.Errorf("inspect resolved protected input path: %w", err)
		}
		if !sameFileIdentity(requestedStat, resolvedStat) {
			return nil, fmt.Errorf("protected input path changed while resolving it")
		}
		inputs = append(inputs, protectedInputSnapshot{
			requestedPath: absolute,
			resolvedPath:  filepath.Clean(resolved),
			fileStat:      resolvedStat,
		})
	}
	return inputs, nil
}

func (output *privateAtomicOutput) verifyProtectedInputs() error {
	if output == nil || output.parent == nil {
		return fmt.Errorf("output parent is not open")
	}
	destinationStat, destinationExists, err := inspectOutputAt(int(output.parent.Fd()), output.base)
	if err != nil {
		return err
	}
	for _, input := range output.protectedInputs {
		resolved, err := filepath.EvalSymlinks(input.requestedPath)
		if err != nil {
			return fmt.Errorf("re-resolve protected input path: %w", err)
		}
		if filepath.Clean(resolved) != input.resolvedPath {
			return fmt.Errorf("protected input resolved path changed before publication")
		}
		requestedStat, err := statFollowingSymlinks(input.requestedPath)
		if err != nil {
			return fmt.Errorf("reinspect protected input path: %w", err)
		}
		resolvedStat, err := statFollowingSymlinks(input.resolvedPath)
		if err != nil {
			return fmt.Errorf("reinspect resolved protected input path: %w", err)
		}
		if !sameFileIdentity(input.fileStat, requestedStat) || !sameFileIdentity(input.fileStat, resolvedStat) {
			return fmt.Errorf("protected input path identity changed before publication")
		}
		if err := output.rejectProtectedInputAlias(destinationStat, destinationExists); err != nil {
			return err
		}
	}
	return nil
}

func (output *privateAtomicOutput) rejectProtectedInputAlias(destinationStat unix.Stat_t, destinationExists bool) error {
	for _, input := range output.protectedInputs {
		if filepath.Clean(output.resolvedOutput) == input.resolvedPath {
			return fmt.Errorf("output path aliases a protected input")
		}
		if destinationExists && sameFileIdentity(destinationStat, input.fileStat) {
			return fmt.Errorf("output file aliases a protected input")
		}
	}
	return nil
}

func statFollowingSymlinks(path string) (unix.Stat_t, error) {
	var stat unix.Stat_t
	if err := unix.Stat(path, &stat); err != nil {
		return unix.Stat_t{}, err
	}
	return stat, nil
}

func (output *privateAtomicOutput) verifyParentIdentity() error {
	if output == nil || output.parent == nil {
		return fmt.Errorf("output parent is not open")
	}
	var opened unix.Stat_t
	if err := unix.Fstat(int(output.parent.Fd()), &opened); err != nil {
		return fmt.Errorf("reinspect opened output parent: %w", err)
	}
	if !sameFileIdentity(output.parentStat, opened) || opened.Mode != output.parentStat.Mode || opened.Uid != output.parentStat.Uid {
		return fmt.Errorf("opened output parent identity or mode changed")
	}
	if err := validatePrivateParent(opened); err != nil {
		return err
	}
	for description, path := range map[string]string{
		"requested output parent": output.requestedParent,
		"resolved output parent":  output.resolvedParent,
	} {
		var current unix.Stat_t
		if err := unix.Stat(path, &current); err != nil {
			return fmt.Errorf("reinspect %s: %w", description, err)
		}
		if !sameFileIdentity(output.parentStat, current) || current.Mode != output.parentStat.Mode || current.Uid != output.parentStat.Uid {
			return fmt.Errorf("%s identity or mode changed", description)
		}
	}
	return nil
}

func (output *privateAtomicOutput) verifyTemporaryIdentity() error {
	if output == nil || output.parent == nil || output.file == nil || output.temporaryName == "" {
		return fmt.Errorf("fresh private output is not open")
	}
	var opened unix.Stat_t
	if err := unix.Fstat(int(output.file.Fd()), &opened); err != nil {
		return fmt.Errorf("reinspect opened fresh output: %w", err)
	}
	if !samePrivateFile(output.fileStat, opened) {
		return fmt.Errorf("opened fresh output identity or attributes changed")
	}
	pathStat, exists, err := inspectOutputAt(int(output.parent.Fd()), output.temporaryName)
	if err != nil {
		return fmt.Errorf("reinspect fresh output path: %w", err)
	}
	if !exists || !samePrivateFile(output.fileStat, pathStat) {
		return fmt.Errorf("fresh output path identity or attributes changed")
	}
	return nil
}

func (output *privateAtomicOutput) verifyDestinationUnchanged() error {
	current, exists, err := inspectOutputAt(int(output.parent.Fd()), output.base)
	if err != nil {
		return err
	}
	if exists != output.destinationSet {
		return fmt.Errorf("output path was replaced before publication")
	}
	if exists && !samePrivateFile(output.destinationStat, current) {
		return fmt.Errorf("output path identity or attributes changed before publication")
	}
	return nil
}

func (output *privateAtomicOutput) verifyPublishedIdentity() error {
	if output == nil || output.parent == nil || output.file == nil || !output.published {
		return fmt.Errorf("private output is not published")
	}
	var opened unix.Stat_t
	if err := unix.Fstat(int(output.file.Fd()), &opened); err != nil {
		return fmt.Errorf("reinspect opened published output: %w", err)
	}
	if !samePrivateFile(output.fileStat, opened) {
		return fmt.Errorf("opened published output identity or attributes changed")
	}
	pathStat, exists, err := inspectOutputAt(int(output.parent.Fd()), output.base)
	if err != nil {
		return fmt.Errorf("reinspect published output path: %w", err)
	}
	if !exists || !samePrivateFile(output.fileStat, pathStat) {
		return fmt.Errorf("published output path identity or attributes changed")
	}
	return nil
}

func (output *privateAtomicOutput) close() error {
	if output == nil {
		return nil
	}
	var result error
	if output.temporaryName != "" && output.parent != nil {
		var opened unix.Stat_t
		if output.file == nil {
			result = errors.Join(result, fmt.Errorf("failed temporary output is not open; refusing cleanup"))
		} else if err := unix.Fstat(int(output.file.Fd()), &opened); err != nil {
			result = errors.Join(result, fmt.Errorf("inspect opened failed temporary output: %w", err))
		} else {
			pathStat, exists, err := inspectOutputAt(int(output.parent.Fd()), output.temporaryName)
			if err != nil {
				result = errors.Join(result, fmt.Errorf("inspect failed temporary output cleanup: %w", err))
			} else if exists && sameFileIdentity(opened, pathStat) {
				if err := unix.Unlinkat(int(output.parent.Fd()), output.temporaryName, 0); err != nil {
					result = errors.Join(result, fmt.Errorf("remove failed temporary output: %w", err))
				}
			} else if exists {
				result = errors.Join(result, fmt.Errorf("failed temporary output path identity changed; refusing cleanup"))
			}
		}
		output.temporaryName = ""
	}
	if output.file != nil {
		if err := output.file.Close(); err != nil {
			result = errors.Join(result, fmt.Errorf("close private output: %w", err))
		}
		output.file = nil
	}
	if output.parent != nil {
		if err := output.parent.Close(); err != nil {
			result = errors.Join(result, fmt.Errorf("close output parent: %w", err))
		}
		output.parent = nil
	}
	return result
}

func inspectOutputAt(parentFD int, name string) (unix.Stat_t, bool, error) {
	var stat unix.Stat_t
	err := unix.Fstatat(parentFD, name, &stat, unix.AT_SYMLINK_NOFOLLOW)
	if errors.Is(err, unix.ENOENT) {
		return unix.Stat_t{}, false, nil
	}
	if err != nil {
		return unix.Stat_t{}, false, fmt.Errorf("inspect output path without following links: %w", err)
	}
	return stat, true, nil
}

func validatePrivateParent(stat unix.Stat_t) error {
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR {
		return fmt.Errorf("output parent must be a directory")
	}
	if stat.Uid != uint32(os.Geteuid()) {
		return fmt.Errorf("output parent must be owned by the current user")
	}
	if stat.Mode&0o022 != 0 {
		return fmt.Errorf("output parent must not be writable by group or others")
	}
	return nil
}

func validatePrivateFile(stat unix.Stat_t, description string) error {
	if stat.Mode&unix.S_IFMT != unix.S_IFREG {
		return fmt.Errorf("%s must be a regular non-symlink file", description)
	}
	if stat.Mode&0o7777 != 0o600 {
		return fmt.Errorf("%s must have exact mode 0600", description)
	}
	if stat.Uid != uint32(os.Geteuid()) {
		return fmt.Errorf("%s must be owned by the current user", description)
	}
	if stat.Nlink != 1 {
		return fmt.Errorf("%s must have exactly one link", description)
	}
	return nil
}

func sameFileIdentity(left, right unix.Stat_t) bool {
	return left.Dev == right.Dev && left.Ino == right.Ino
}

func samePrivateFile(left, right unix.Stat_t) bool {
	return sameFileIdentity(left, right) && left.Mode == right.Mode && left.Uid == right.Uid && left.Nlink == right.Nlink && right.Mode&unix.S_IFMT == unix.S_IFREG && right.Mode&0o7777 == 0o600 && right.Nlink == 1
}

func randomAtomicOutputName() (string, error) {
	var entropy [16]byte
	if _, err := io.ReadFull(rand.Reader, entropy[:]); err != nil {
		return "", fmt.Errorf("generate fresh private output name: %w", err)
	}
	return fmt.Sprintf(".fugue-release-domain-plan-%x.tmp", entropy[:]), nil
}

func writeAll(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		written, err := writer.Write(data)
		if err != nil {
			return err
		}
		if written <= 0 {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}
