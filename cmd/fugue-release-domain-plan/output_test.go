package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func TestWritePrivateAtomicFileCreatesAndReplacesPrivateOutput(t *testing.T) {
	directory := t.TempDir()
	outputPath := filepath.Join(directory, "release-domain-plan.json")

	if err := writePrivateAtomicFile(outputPath, []byte("first\n")); err != nil {
		t.Fatalf("create private output: %v", err)
	}
	first := statPrivateTestFile(t, outputPath)
	assertFileContents(t, outputPath, "first\n")
	assertNoAtomicOutputTemps(t, directory)

	if err := writePrivateAtomicFile(outputPath, []byte("second\n")); err != nil {
		t.Fatalf("replace private output: %v", err)
	}
	second := statPrivateTestFile(t, outputPath)
	assertFileContents(t, outputPath, "second\n")
	assertNoAtomicOutputTemps(t, directory)
	if sameFileIdentity(first, second) {
		t.Fatal("replacement reused the existing output inode instead of publishing a fresh file")
	}
}

func TestWritePrivateAtomicFileRejectsUnsafeExistingTargets(t *testing.T) {
	t.Run("symbolic link", func(t *testing.T) {
		directory := t.TempDir()
		target := filepath.Join(directory, "target")
		outputPath := filepath.Join(directory, "output")
		if err := os.WriteFile(target, []byte("protected"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, outputPath); err != nil {
			t.Fatal(err)
		}
		if err := writePrivateAtomicFile(outputPath, []byte("replacement")); err == nil {
			t.Fatal("symbolic-link output unexpectedly succeeded")
		}
		assertFileContents(t, target, "protected")
		if info, err := os.Lstat(outputPath); err != nil || info.Mode()&os.ModeSymlink == 0 {
			t.Fatalf("output symlink changed: info=%v err=%v", info, err)
		}
		assertNoAtomicOutputTemps(t, directory)
	})

	t.Run("directory", func(t *testing.T) {
		directory := t.TempDir()
		outputPath := filepath.Join(directory, "output")
		if err := os.Mkdir(outputPath, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := writePrivateAtomicFile(outputPath, []byte("replacement")); err == nil {
			t.Fatal("directory output unexpectedly succeeded")
		}
		if info, err := os.Stat(outputPath); err != nil || !info.IsDir() {
			t.Fatalf("output directory changed: info=%v err=%v", info, err)
		}
		assertNoAtomicOutputTemps(t, directory)
	})

	t.Run("hard link", func(t *testing.T) {
		directory := t.TempDir()
		target := filepath.Join(directory, "target")
		outputPath := filepath.Join(directory, "output")
		if err := os.WriteFile(target, []byte("protected"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(target, outputPath); err != nil {
			t.Fatal(err)
		}
		if err := writePrivateAtomicFile(outputPath, []byte("replacement")); err == nil {
			t.Fatal("hard-linked output unexpectedly succeeded")
		}
		assertFileContents(t, target, "protected")
		assertFileContents(t, outputPath, "protected")
		assertNoAtomicOutputTemps(t, directory)
	})

	t.Run("dangerous mode", func(t *testing.T) {
		directory := t.TempDir()
		outputPath := filepath.Join(directory, "output")
		if err := os.WriteFile(outputPath, []byte("protected"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(outputPath, 0o644); err != nil {
			t.Fatal(err)
		}
		if err := writePrivateAtomicFile(outputPath, []byte("replacement")); err == nil {
			t.Fatal("non-private output unexpectedly succeeded")
		}
		assertFileContents(t, outputPath, "protected")
		info, err := os.Stat(outputPath)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o644 {
			t.Fatalf("unsafe output mode was mutated: %04o", info.Mode().Perm())
		}
		assertNoAtomicOutputTemps(t, directory)
	})
}

func TestWritePrivateAtomicFileRejectsProtectedInputAliases(t *testing.T) {
	t.Run("direct path", func(t *testing.T) {
		directory := t.TempDir()
		inputPath := filepath.Join(directory, "input")
		if err := os.WriteFile(inputPath, []byte("protected"), 0o600); err != nil {
			t.Fatal(err)
		}

		err := writePrivateAtomicFile(inputPath, []byte("replacement"), inputPath)
		if err == nil || !strings.Contains(err.Error(), "aliases a protected input") {
			t.Fatalf("direct protected-input alias error = %v", err)
		}
		assertFileContents(t, inputPath, "protected")
		assertNoAtomicOutputTemps(t, directory)
	})

	t.Run("resolved parent symlink", func(t *testing.T) {
		root := t.TempDir()
		directory := filepath.Join(root, "actual")
		if err := os.Mkdir(directory, 0o700); err != nil {
			t.Fatal(err)
		}
		aliasParent := filepath.Join(root, "alias")
		if err := os.Symlink(directory, aliasParent); err != nil {
			t.Fatal(err)
		}
		inputPath := filepath.Join(directory, "input")
		if err := os.WriteFile(inputPath, []byte("protected"), 0o600); err != nil {
			t.Fatal(err)
		}

		err := writePrivateAtomicFile(filepath.Join(aliasParent, "input"), []byte("replacement"), inputPath)
		if err == nil || !strings.Contains(err.Error(), "aliases a protected input") {
			t.Fatalf("resolved-parent protected-input alias error = %v", err)
		}
		assertFileContents(t, inputPath, "protected")
		assertNoAtomicOutputTemps(t, directory)
	})

	t.Run("hard link", func(t *testing.T) {
		directory := t.TempDir()
		inputPath := filepath.Join(directory, "input")
		outputPath := filepath.Join(directory, "output")
		if err := os.WriteFile(inputPath, []byte("protected"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(inputPath, outputPath); err != nil {
			t.Fatal(err)
		}

		err := writePrivateAtomicFile(outputPath, []byte("replacement"), inputPath)
		if err == nil || !strings.Contains(err.Error(), "aliases a protected input") {
			t.Fatalf("hard-linked protected-input alias error = %v", err)
		}
		assertFileContents(t, inputPath, "protected")
		assertFileContents(t, outputPath, "protected")
		assertNoAtomicOutputTemps(t, directory)
	})
}

func TestWritePrivateAtomicFileRejectsUnsafeParent(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o777); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(directory, 0o700) })
	outputPath := filepath.Join(directory, "output")
	if err := writePrivateAtomicFile(outputPath, []byte("private")); err == nil {
		t.Fatal("output in group/world-writable parent unexpectedly succeeded")
	}
	if _, err := os.Lstat(outputPath); !os.IsNotExist(err) {
		t.Fatalf("unsafe parent received output: %v", err)
	}
	assertNoAtomicOutputTemps(t, directory)
}

func TestPrivateAtomicOutputRejectsDestinationReplacement(t *testing.T) {
	directory := t.TempDir()
	outputPath := filepath.Join(directory, "output")
	displacedPath := filepath.Join(directory, "displaced")
	if err := os.WriteFile(outputPath, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}

	output, err := createPrivateAtomicOutput(outputPath)
	if err != nil {
		t.Fatalf("prepare private output: %v", err)
	}
	defer func() {
		if err := output.close(); err != nil {
			t.Errorf("close private output: %v", err)
		}
	}()
	if err := os.Rename(outputPath, displacedPath); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(outputPath, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}

	err = output.publish([]byte("planner-data"))
	if err == nil || !strings.Contains(err.Error(), "identity or attributes changed") {
		t.Fatalf("publish after destination replacement error = %v", err)
	}
	assertFileContents(t, outputPath, "replacement")
	assertFileContents(t, displacedPath, "original")
}

func TestPrivateAtomicOutputRejectsDestinationCreation(t *testing.T) {
	directory := t.TempDir()
	outputPath := filepath.Join(directory, "output")
	output, err := createPrivateAtomicOutput(outputPath)
	if err != nil {
		t.Fatalf("prepare private output: %v", err)
	}
	defer func() {
		if err := output.close(); err != nil {
			t.Errorf("close private output: %v", err)
		}
	}()
	if err := os.WriteFile(outputPath, []byte("intruder"), 0o600); err != nil {
		t.Fatal(err)
	}

	err = output.publish([]byte("planner-data"))
	if err == nil || !strings.Contains(err.Error(), "was replaced") {
		t.Fatalf("publish after destination creation error = %v", err)
	}
	assertFileContents(t, outputPath, "intruder")
}

func TestPrivateAtomicOutputRejectsParentReplacement(t *testing.T) {
	root := t.TempDir()
	directory := filepath.Join(root, "evidence")
	displaced := filepath.Join(root, "displaced")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	output, err := createPrivateAtomicOutput(filepath.Join(directory, "output"))
	if err != nil {
		t.Fatalf("prepare private output: %v", err)
	}
	defer func() {
		if err := output.close(); err != nil {
			t.Errorf("close private output: %v", err)
		}
	}()
	if err := os.Rename(directory, displaced); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}

	err = output.publish([]byte("planner-data"))
	if err == nil || !strings.Contains(err.Error(), "parent identity or mode changed") {
		t.Fatalf("publish after parent replacement error = %v", err)
	}
	if _, err := os.Lstat(filepath.Join(directory, "output")); !os.IsNotExist(err) {
		t.Fatalf("replacement parent received output: %v", err)
	}
}

func TestPrivateAtomicOutputRejectsProtectedInputIdentityRace(t *testing.T) {
	directory := t.TempDir()
	inputPath := filepath.Join(directory, "input")
	displacedPath := filepath.Join(directory, "displaced")
	outputPath := filepath.Join(directory, "output")
	if err := os.WriteFile(inputPath, []byte("original-input"), 0o600); err != nil {
		t.Fatal(err)
	}

	output, err := createPrivateAtomicOutput(outputPath, inputPath)
	if err != nil {
		t.Fatalf("prepare private output: %v", err)
	}
	if err := os.Rename(inputPath, displacedPath); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(inputPath, []byte("replacement-input"), 0o600); err != nil {
		t.Fatal(err)
	}

	err = output.publish([]byte("planner-data"))
	if err == nil || !strings.Contains(err.Error(), "protected input path identity changed") {
		t.Fatalf("publish after protected-input identity race error = %v", err)
	}
	if err := output.close(); err != nil {
		t.Fatalf("close private output: %v", err)
	}
	assertFileContents(t, inputPath, "replacement-input")
	assertFileContents(t, displacedPath, "original-input")
	if _, err := os.Lstat(outputPath); !os.IsNotExist(err) {
		t.Fatalf("output was published after protected-input identity race: %v", err)
	}
	assertNoAtomicOutputTemps(t, directory)
}

func TestPrivateAtomicOutputRejectsProtectedInputResolvedParentRace(t *testing.T) {
	root := t.TempDir()
	inputDirectory := filepath.Join(root, "input-directory")
	outputDirectory := filepath.Join(root, "output-directory")
	for _, directory := range []string{inputDirectory, outputDirectory} {
		if err := os.Mkdir(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	inputPath := filepath.Join(inputDirectory, "shared-name")
	outputPath := filepath.Join(outputDirectory, "shared-name")
	if err := os.WriteFile(inputPath, []byte("protected-input"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(outputPath, []byte("original-output"), 0o600); err != nil {
		t.Fatal(err)
	}
	inputAliasParent := filepath.Join(root, "input-alias")
	if err := os.Symlink(inputDirectory, inputAliasParent); err != nil {
		t.Fatal(err)
	}
	protectedPath := filepath.Join(inputAliasParent, "shared-name")

	output, err := createPrivateAtomicOutput(outputPath, protectedPath)
	if err != nil {
		t.Fatalf("prepare private output: %v", err)
	}
	if err := os.Remove(inputAliasParent); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outputDirectory, inputAliasParent); err != nil {
		t.Fatal(err)
	}

	err = output.publish([]byte("planner-data"))
	if err == nil || !strings.Contains(err.Error(), "protected input resolved path changed") {
		t.Fatalf("publish after protected-input parent race error = %v", err)
	}
	if err := output.close(); err != nil {
		t.Fatalf("close private output: %v", err)
	}
	assertFileContents(t, inputPath, "protected-input")
	assertFileContents(t, outputPath, "original-output")
	assertNoAtomicOutputTemps(t, outputDirectory)
}

func statPrivateTestFile(t *testing.T, path string) unix.Stat_t {
	t.Helper()
	var stat unix.Stat_t
	if err := unix.Lstat(path, &stat); err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if err := validatePrivateFile(stat, "test output"); err != nil {
		t.Fatal(err)
	}
	return stat
}

func assertFileContents(t *testing.T, path, expected string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if string(data) != expected {
		t.Fatalf("contents of %s = %q, want %q", path, data, expected)
	}
}

func assertNoAtomicOutputTemps(t *testing.T, directory string) {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatalf("read output directory: %v", err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".fugue-release-domain-plan-") {
			t.Errorf("private output temporary file was left behind: %s", entry.Name())
		}
	}
}
