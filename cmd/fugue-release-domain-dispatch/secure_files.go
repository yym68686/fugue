package main

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"golang.org/x/sys/unix"
)

const atomicCreateAttempts = 128

type fileIdentity struct {
	dev       uint64
	ino       uint64
	mode      uint32
	nlink     uint64
	uid       uint32
	size      int64
	mtimeSec  int64
	mtimeNsec int64
	ctimeSec  int64
	ctimeNsec int64
}

type privateBundle struct {
	requestedParent string
	resolvedParent  string
	base            string
	parent          *os.File
	parentIdentity  fileIdentity
	directory       *os.File
	directoryID     fileIdentity
	files           map[string]fileIdentity
	listed          bool
}

func readSecureSource(path string, limit int64, exactPrivateMode bool) ([]byte, error) {
	return readSecureSourceWithPolicy(path, limit, exactPrivateMode, false)
}

func readSecureTraceSource(path string, limit int64) ([]byte, error) {
	return readSecureSourceWithPolicy(path, limit, true, true)
}

func readSecureSourceWithPolicy(path string, limit int64, exactPrivateMode, exactPrivateParent bool) ([]byte, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve source path: %w", err)
	}
	base := filepath.Base(absolute)
	if base == "." || base == ".." || base == string(filepath.Separator) {
		return nil, fmt.Errorf("source path must name one file")
	}
	requestedParent := filepath.Dir(absolute)
	resolvedParent, err := filepath.EvalSymlinks(requestedParent)
	if err != nil {
		return nil, fmt.Errorf("resolve source parent: %w", err)
	}
	parentFD, err := unix.Open(resolvedParent, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("open source parent: %w", err)
	}
	parent := os.NewFile(uintptr(parentFD), resolvedParent)
	if parent == nil {
		_ = unix.Close(parentFD)
		return nil, fmt.Errorf("adopt source parent")
	}
	defer parent.Close()
	parentID, err := fstatIdentity(parentFD)
	if err != nil {
		return nil, fmt.Errorf("inspect source parent: %w", err)
	}
	if err := verifyDirectoryPath(requestedParent, resolvedParent, parentID); err != nil {
		return nil, err
	}
	if exactPrivateParent {
		if err := validatePrivateDirectory(parentID); err != nil {
			return nil, err
		}
		var requested unix.Stat_t
		if err := unix.Lstat(requestedParent, &requested); err != nil || requested.Mode&unix.S_IFMT == unix.S_IFLNK || !sameDirectoryIdentity(statIdentity(requested), parentID) {
			return nil, fmt.Errorf("private source parent must be an exact non-symlink directory")
		}
	}

	fd, err := unix.Openat(parentFD, base, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("open source without following symlinks: %w", err)
	}
	file := os.NewFile(uintptr(fd), base)
	if file == nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("adopt source file")
	}
	defer file.Close()
	identity, err := fstatIdentity(fd)
	if err != nil {
		return nil, fmt.Errorf("inspect source file: %w", err)
	}
	if err := validatePrivateRegular(identity, limit, exactPrivateMode); err != nil {
		return nil, err
	}
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, fmt.Errorf("read source: %w", err)
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("source exceeds %d-byte limit", limit)
	}
	if int64(len(data)) != identity.size {
		return nil, fmt.Errorf("source size changed while reading")
	}
	openedAfter, err := fstatIdentity(fd)
	if err != nil {
		return nil, fmt.Errorf("reinspect source file: %w", err)
	}
	pathAfter, err := fstatatIdentity(parentFD, base)
	if err != nil {
		return nil, fmt.Errorf("reinspect source path: %w", err)
	}
	if identity != openedAfter || identity != pathAfter {
		return nil, fmt.Errorf("source identity or attributes changed while reading")
	}
	if err := verifyDirectoryPath(requestedParent, resolvedParent, parentID); err != nil {
		return nil, err
	}
	if exactPrivateParent {
		var requested unix.Stat_t
		if err := unix.Lstat(requestedParent, &requested); err != nil || requested.Mode&unix.S_IFMT == unix.S_IFLNK || !sameDirectoryIdentity(statIdentity(requested), parentID) {
			return nil, fmt.Errorf("private source parent identity changed while reading")
		}
	}
	return data, nil
}

func createFreshPrivateBundle(path string) (*privateBundle, error) {
	bundle, err := openBundleParent(path)
	if err != nil {
		return nil, err
	}
	fail := func(cause error) (*privateBundle, error) {
		return nil, joinErrors(cause, bundle.close())
	}
	if _, err := fstatatIdentity(int(bundle.parent.Fd()), bundle.base); err == nil {
		return fail(fmt.Errorf("bundle directory already exists"))
	} else if !errors.Is(err, unix.ENOENT) {
		return fail(fmt.Errorf("inspect bundle destination: %w", err))
	}
	if err := unix.Mkdirat(int(bundle.parent.Fd()), bundle.base, 0o700); err != nil {
		return fail(fmt.Errorf("create fresh bundle directory: %w", err))
	}
	if err := bundle.parent.Sync(); err != nil {
		return fail(fmt.Errorf("sync bundle parent: %w", err))
	}
	directoryFD, err := unix.Openat(int(bundle.parent.Fd()), bundle.base, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return fail(fmt.Errorf("open fresh bundle directory: %w", err))
	}
	bundle.directory = os.NewFile(uintptr(directoryFD), bundle.base)
	if bundle.directory == nil {
		_ = unix.Close(directoryFD)
		return fail(fmt.Errorf("adopt fresh bundle directory"))
	}
	bundle.directoryID, err = fstatIdentity(directoryFD)
	if err != nil {
		return fail(fmt.Errorf("inspect fresh bundle directory: %w", err))
	}
	if err := validatePrivateDirectory(bundle.directoryID); err != nil {
		return fail(err)
	}
	if err := bundle.verifyDirectoryIdentity(); err != nil {
		return fail(err)
	}
	return bundle, nil
}

func openPrivateBundle(path string) (*privateBundle, error) {
	bundle, err := openBundleParent(path)
	if err != nil {
		return nil, err
	}
	fail := func(cause error) (*privateBundle, error) {
		return nil, joinErrors(cause, bundle.close())
	}
	directoryFD, err := unix.Openat(int(bundle.parent.Fd()), bundle.base, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return fail(fmt.Errorf("open bundle directory without following symlinks: %w", err))
	}
	bundle.directory = os.NewFile(uintptr(directoryFD), bundle.base)
	if bundle.directory == nil {
		_ = unix.Close(directoryFD)
		return fail(fmt.Errorf("adopt bundle directory"))
	}
	bundle.directoryID, err = fstatIdentity(directoryFD)
	if err != nil {
		return fail(fmt.Errorf("inspect bundle directory: %w", err))
	}
	if err := validatePrivateDirectory(bundle.directoryID); err != nil {
		return fail(err)
	}
	if err := bundle.verifyDirectoryIdentity(); err != nil {
		return fail(err)
	}
	return bundle, nil
}

func openBundleParent(path string) (*privateBundle, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve bundle path: %w", err)
	}
	base := filepath.Base(absolute)
	if base == "." || base == ".." || base == string(filepath.Separator) {
		return nil, fmt.Errorf("bundle path must name one directory")
	}
	requestedParent := filepath.Dir(absolute)
	resolvedParent, err := filepath.EvalSymlinks(requestedParent)
	if err != nil {
		return nil, fmt.Errorf("resolve bundle parent: %w", err)
	}
	parentFD, err := unix.Open(resolvedParent, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("open bundle parent: %w", err)
	}
	parent := os.NewFile(uintptr(parentFD), resolvedParent)
	if parent == nil {
		_ = unix.Close(parentFD)
		return nil, fmt.Errorf("adopt bundle parent")
	}
	parentID, err := fstatIdentity(parentFD)
	if err != nil {
		_ = parent.Close()
		return nil, fmt.Errorf("inspect bundle parent: %w", err)
	}
	if err := verifyDirectoryPath(requestedParent, resolvedParent, parentID); err != nil {
		_ = parent.Close()
		return nil, err
	}
	return &privateBundle{
		requestedParent: requestedParent,
		resolvedParent:  resolvedParent,
		base:            base,
		parent:          parent,
		parentIdentity:  parentID,
		files:           map[string]fileIdentity{},
	}, nil
}

func (bundle *privateBundle) writeAtomic(name string, data []byte) error {
	if err := validateBundleName(name); err != nil {
		return err
	}
	if bundle == nil || bundle.directory == nil {
		return fmt.Errorf("bundle is not open")
	}
	if _, exists := bundle.files[name]; exists {
		return fmt.Errorf("bundle file %q already published", name)
	}
	if _, err := fstatatIdentity(int(bundle.directory.Fd()), name); err == nil {
		return fmt.Errorf("bundle file %q already exists", name)
	} else if !errors.Is(err, unix.ENOENT) {
		return fmt.Errorf("inspect bundle file %q: %w", name, err)
	}
	for attempt := 0; attempt < atomicCreateAttempts; attempt++ {
		temporary, err := randomTemporaryName()
		if err != nil {
			return err
		}
		fd, err := unix.Openat(int(bundle.directory.Fd()), temporary, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
		if errors.Is(err, unix.EEXIST) {
			continue
		}
		if err != nil {
			return fmt.Errorf("create atomic bundle file: %w", err)
		}
		file := os.NewFile(uintptr(fd), temporary)
		if file == nil {
			_ = unix.Close(fd)
			_ = unix.Unlinkat(int(bundle.directory.Fd()), temporary, 0)
			return fmt.Errorf("adopt atomic bundle file")
		}
		published := false
		cleanup := func() {
			_ = file.Close()
			if !published {
				_ = unix.Unlinkat(int(bundle.directory.Fd()), temporary, 0)
			}
		}
		if err := unix.Fchmod(fd, 0o600); err != nil {
			cleanup()
			return fmt.Errorf("set bundle file mode: %w", err)
		}
		if err := writeAll(file, data); err != nil {
			cleanup()
			return fmt.Errorf("write bundle file: %w", err)
		}
		if err := file.Sync(); err != nil {
			cleanup()
			return fmt.Errorf("sync bundle file: %w", err)
		}
		identity, err := fstatIdentity(fd)
		if err != nil {
			cleanup()
			return fmt.Errorf("inspect bundle file: %w", err)
		}
		if err := validatePrivateRegular(identity, int64(len(data)), true); err != nil || identity.size != int64(len(data)) {
			cleanup()
			if err != nil {
				return err
			}
			return fmt.Errorf("bundle file size mismatch")
		}
		pathIdentity, err := fstatatIdentity(int(bundle.directory.Fd()), temporary)
		if err != nil || pathIdentity != identity {
			cleanup()
			return fmt.Errorf("atomic bundle file identity changed before publication")
		}
		if _, err := fstatatIdentity(int(bundle.directory.Fd()), name); err == nil {
			cleanup()
			return fmt.Errorf("bundle destination appeared before publication")
		} else if !errors.Is(err, unix.ENOENT) {
			cleanup()
			return fmt.Errorf("inspect bundle destination: %w", err)
		}
		// Linkat is an atomic no-replace publication primitive here: unlike
		// rename(2), it cannot overwrite a destination that appears after our
		// existence check. Removing the temporary link restores the required
		// single-link invariant before the file can be accepted.
		if err := unix.Linkat(int(bundle.directory.Fd()), temporary, int(bundle.directory.Fd()), name, 0); err != nil {
			cleanup()
			return fmt.Errorf("atomically publish bundle file without replacement: %w", err)
		}
		if err := unix.Unlinkat(int(bundle.directory.Fd()), temporary, 0); err != nil {
			_ = unix.Unlinkat(int(bundle.directory.Fd()), name, 0)
			cleanup()
			return fmt.Errorf("remove atomic bundle temporary link: %w", err)
		}
		published = true
		if err := file.Sync(); err != nil {
			cleanup()
			return fmt.Errorf("sync published bundle inode: %w", err)
		}
		identity, err = fstatIdentity(fd)
		if err != nil {
			cleanup()
			return fmt.Errorf("reinspect published bundle inode: %w", err)
		}
		if err := validatePrivateRegular(identity, int64(len(data)), true); err != nil || identity.size != int64(len(data)) {
			cleanup()
			if err != nil {
				return err
			}
			return fmt.Errorf("published bundle inode size mismatch")
		}
		if err := bundle.directory.Sync(); err != nil {
			cleanup()
			return fmt.Errorf("sync bundle directory: %w", err)
		}
		publishedIdentity, err := fstatatIdentity(int(bundle.directory.Fd()), name)
		if err != nil || publishedIdentity != identity {
			cleanup()
			return fmt.Errorf("published bundle file identity changed")
		}
		cleanup()
		bundle.files[name] = identity
		return bundle.verifyDirectoryIdentity()
	}
	return fmt.Errorf("create atomic bundle file: exhausted collision attempts")
}

func (bundle *privateBundle) read(name string, limit int64) ([]byte, error) {
	if err := validateBundleName(name); err != nil {
		return nil, err
	}
	if bundle == nil || bundle.directory == nil {
		return nil, fmt.Errorf("bundle is not open")
	}
	fd, err := unix.Openat(int(bundle.directory.Fd()), name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("securely open bundle file %q: %w", name, err)
	}
	file := os.NewFile(uintptr(fd), name)
	if file == nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("adopt bundle file %q", name)
	}
	defer file.Close()
	identity, err := fstatIdentity(fd)
	if err != nil {
		return nil, fmt.Errorf("inspect bundle file %q: %w", name, err)
	}
	if err := validatePrivateRegular(identity, limit, true); err != nil {
		return nil, fmt.Errorf("bundle file %q: %w", name, err)
	}
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, fmt.Errorf("read bundle file %q: %w", name, err)
	}
	if int64(len(data)) > limit || int64(len(data)) != identity.size {
		return nil, fmt.Errorf("bundle file %q size changed or exceeds limit", name)
	}
	openedAfter, err := fstatIdentity(fd)
	if err != nil {
		return nil, fmt.Errorf("reinspect bundle file %q: %w", name, err)
	}
	pathAfter, err := fstatatIdentity(int(bundle.directory.Fd()), name)
	if err != nil {
		return nil, fmt.Errorf("reinspect bundle file path %q: %w", name, err)
	}
	if identity != openedAfter || identity != pathAfter {
		return nil, fmt.Errorf("bundle file %q identity or attributes changed while reading", name)
	}
	bundle.files[name] = identity
	return data, nil
}

func (bundle *privateBundle) verifyNames(expected map[string]struct{}) error {
	if bundle.listed {
		return fmt.Errorf("bundle directory was already enumerated")
	}
	names, err := bundle.directory.Readdirnames(-1)
	if err != nil {
		return fmt.Errorf("enumerate bundle directory: %w", err)
	}
	bundle.listed = true
	return compareBundleNames(names, expected)
}

func (bundle *privateBundle) verifyStable(expected map[string]struct{}) error {
	if err := bundle.verifyDirectoryIdentity(); err != nil {
		return err
	}
	for name, identity := range bundle.files {
		current, err := fstatatIdentity(int(bundle.directory.Fd()), name)
		if err != nil {
			return fmt.Errorf("reinspect bundle file %q: %w", name, err)
		}
		if current != identity {
			return fmt.Errorf("bundle file %q identity or attributes changed", name)
		}
	}
	fd, err := unix.Openat(int(bundle.parent.Fd()), bundle.base, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return fmt.Errorf("reopen bundle directory: %w", err)
	}
	reopened := os.NewFile(uintptr(fd), bundle.base)
	if reopened == nil {
		_ = unix.Close(fd)
		return fmt.Errorf("adopt reopened bundle directory")
	}
	defer reopened.Close()
	reopenedID, err := fstatIdentity(fd)
	if err != nil || !sameDirectoryIdentity(reopenedID, bundle.directoryID) {
		return fmt.Errorf("bundle directory identity or attributes changed")
	}
	names, err := reopened.Readdirnames(-1)
	if err != nil {
		return fmt.Errorf("reenumerate bundle directory: %w", err)
	}
	return compareBundleNames(names, expected)
}

func compareBundleNames(names []string, expected map[string]struct{}) error {
	sort.Strings(names)
	want := make([]string, 0, len(expected))
	for name := range expected {
		want = append(want, name)
	}
	sort.Strings(want)
	if len(names) != len(want) {
		return fmt.Errorf("bundle contains unexpected or missing files")
	}
	for index := range names {
		if names[index] != want[index] {
			return fmt.Errorf("bundle contains unexpected or missing files")
		}
	}
	return nil
}

func (bundle *privateBundle) verifyDirectoryIdentity() error {
	if bundle == nil || bundle.parent == nil || bundle.directory == nil {
		return fmt.Errorf("bundle is not open")
	}
	parentCurrent, err := fstatIdentity(int(bundle.parent.Fd()))
	if err != nil || !sameDirectoryIdentity(parentCurrent, bundle.parentIdentity) {
		return fmt.Errorf("bundle parent identity or attributes changed")
	}
	if err := verifyDirectoryPath(bundle.requestedParent, bundle.resolvedParent, bundle.parentIdentity); err != nil {
		return err
	}
	directoryCurrent, err := fstatIdentity(int(bundle.directory.Fd()))
	if err != nil || !sameDirectoryIdentity(directoryCurrent, bundle.directoryID) {
		return fmt.Errorf("opened bundle directory identity or attributes changed")
	}
	if err := validatePrivateDirectory(directoryCurrent); err != nil {
		return err
	}
	pathCurrent, err := fstatatIdentity(int(bundle.parent.Fd()), bundle.base)
	if err != nil || !sameDirectoryIdentity(pathCurrent, bundle.directoryID) {
		return fmt.Errorf("bundle directory path identity or attributes changed")
	}
	return nil
}

func (bundle *privateBundle) close() error {
	if bundle == nil {
		return nil
	}
	var result error
	if bundle.directory != nil {
		result = joinErrors(result, bundle.directory.Close())
		bundle.directory = nil
	}
	if bundle.parent != nil {
		result = joinErrors(result, bundle.parent.Close())
		bundle.parent = nil
	}
	return result
}

func validatePrivateRegular(identity fileIdentity, limit int64, exactMode bool) error {
	if identity.mode&unix.S_IFMT != unix.S_IFREG {
		return fmt.Errorf("must be a regular file")
	}
	if identity.uid != uint32(os.Geteuid()) {
		return fmt.Errorf("must be owned by the current user")
	}
	if identity.nlink != 1 {
		return fmt.Errorf("must have exactly one hard link")
	}
	if exactMode && identity.mode&0o7777 != 0o600 {
		return fmt.Errorf("must have exact mode 0600")
	}
	if !exactMode && identity.mode&0o022 != 0 {
		return fmt.Errorf("must not be group/world writable")
	}
	if identity.size < 0 || identity.size > limit {
		return fmt.Errorf("size exceeds %d-byte limit", limit)
	}
	return nil
}

func validatePrivateDirectory(identity fileIdentity) error {
	if identity.mode&unix.S_IFMT != unix.S_IFDIR {
		return fmt.Errorf("bundle must be a directory")
	}
	if identity.uid != uint32(os.Geteuid()) {
		return fmt.Errorf("bundle directory must be owned by the current user")
	}
	if identity.mode&0o7777 != 0o700 {
		return fmt.Errorf("bundle directory must have exact mode 0700")
	}
	return nil
}

func validateBundleName(name string) error {
	if name == "" || filepath.Base(name) != name || name == "." || name == ".." {
		return fmt.Errorf("invalid fixed bundle filename")
	}
	return nil
}

func verifyDirectoryPath(requested, resolved string, identity fileIdentity) error {
	for _, path := range []string{requested, resolved} {
		var stat unix.Stat_t
		if err := unix.Stat(path, &stat); err != nil {
			return fmt.Errorf("reinspect directory path: %w", err)
		}
		if !sameDirectoryIdentity(statIdentity(stat), identity) {
			return fmt.Errorf("directory path identity or attributes changed")
		}
	}
	return nil
}

func sameDirectoryIdentity(left, right fileIdentity) bool {
	return left.dev == right.dev &&
		left.ino == right.ino &&
		left.mode == right.mode &&
		left.uid == right.uid
}

func fstatIdentity(fd int) (fileIdentity, error) {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return fileIdentity{}, err
	}
	return statIdentity(stat), nil
}

func fstatatIdentity(dirFD int, name string) (fileIdentity, error) {
	var stat unix.Stat_t
	if err := unix.Fstatat(dirFD, name, &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return fileIdentity{}, err
	}
	return statIdentity(stat), nil
}

func statIdentity(stat unix.Stat_t) fileIdentity {
	mtimeSec, mtimeNsec, ctimeSec, ctimeNsec := statChangeTimes(stat)
	return fileIdentity{
		dev:       uint64(stat.Dev),
		ino:       uint64(stat.Ino),
		mode:      uint32(stat.Mode),
		nlink:     uint64(stat.Nlink),
		uid:       uint32(stat.Uid),
		size:      stat.Size,
		mtimeSec:  mtimeSec,
		mtimeNsec: mtimeNsec,
		ctimeSec:  ctimeSec,
		ctimeNsec: ctimeNsec,
	}
}

func writeAll(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		written, err := writer.Write(data)
		if err != nil {
			return err
		}
		if written <= 0 || written > len(data) {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}

func randomTemporaryName() (string, error) {
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return "", fmt.Errorf("read secure randomness: %w", err)
	}
	return ".fugue-release-domain-" + hex.EncodeToString(random), nil
}
