package snapshotnative

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/longhorn/sparse-tools/sparse"
)

type progress struct{}

func (progress) UpdateFileHandlingProgress(int, bool, error) {}

// Run on Linux/ext4 CI using actual upstream FoldFile, not a Python imitation.
// Verify overlap overwrite, parent-only data, sparse growth and child replace.
func TestPinnedNativeFoldRetainsDataWithinWholeChildBudget(t *testing.T) {
	dir := t.TempDir()
	var filesystem syscall.Statfs_t
	if err := syscall.Statfs(dir, &filesystem); err != nil || filesystem.Type != 0xef53 {
		t.Fatalf("native budget test requires ext4: type=%x error=%v", filesystem.Type, err)
	}
	parent, child := filepath.Join(dir, "parent"), filepath.Join(dir, "child")
	const block, size = 4096, 16 << 20
	create := func(name string, writes map[int64]byte) {
		t.Helper()
		f, err := os.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if err := f.Truncate(size); err != nil {
			t.Fatal(err)
		}
		for offset, value := range writes {
			if _, err := f.WriteAt(bytes.Repeat([]byte{value}, block), offset); err != nil {
				t.Fatal(err)
			}
		}
		if err := f.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	allocated := func(name string) int64 {
		t.Helper()
		info, err := os.Stat(name)
		if err != nil {
			t.Fatal(err)
		}
		return info.Sys().(*syscall.Stat_t).Blocks * 512
	}
	create(parent, map[int64]byte{0: 1, block * 2: 2})
	create(child, map[int64]byte{block * 2: 3, block * 4: 4})
	before, bound := allocated(parent), allocated(child)
	if err := sparse.FoldFile(child, parent, progress{}); err != nil {
		t.Fatal(err)
	}
	if growth := allocated(parent) - before; growth > bound+1<<20 {
		t.Fatalf("allocation growth %d exceeded whole-child budget %d", growth, bound)
	}
	if err := os.Remove(child); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(parent, child); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(parent); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(child)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for offset, value := range map[int64]byte{0: 1, block: 0, block * 2: 3, block * 4: 4, size - block: 0} {
		got := make([]byte, block)
		if _, err := f.ReadAt(got, offset); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, bytes.Repeat([]byte{value}, block)) {
			t.Fatalf("retained view changed at %d", offset)
		}
	}
}

// Exercise rolling topology changes, including zero overwrites, using the
// actual four-worker fold implementation. A protected ancestor is never folded.
func TestFifteenNativeFoldsPreserveProtectedAndNewestViews(t *testing.T) {
	dir := t.TempDir()
	const block, blocks = 4096, 64
	type layer struct {
		path string
		data map[int]byte
	}
	layers := make([]layer, 17)
	expected := make([]byte, blocks)
	for i := range layers {
		layers[i] = layer{filepath.Join(dir, fmt.Sprint(i)), map[int]byte{}}
		if i == 0 {
			for offset := 0; offset < blocks; offset++ {
				layers[i].data[offset] = 1
			}
		} else {
			layers[i].data[(i*3)%blocks] = byte(i)
			layers[i].data[(i*7)%blocks] = 0
		}
		f, err := os.Create(layers[i].path)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.Truncate(block * blocks); err != nil {
			t.Fatal(err)
		}
		for offset, value := range layers[i].data {
			if _, err := f.WriteAt(bytes.Repeat([]byte{value}, block), int64(offset*block)); err != nil {
				t.Fatal(err)
			}
			expected[offset] = value
		}
		if err := f.Sync(); err != nil {
			t.Fatal(err)
		}
		f.Close()
	}
	protected, err := os.ReadFile(layers[0].path)
	if err != nil {
		t.Fatal(err)
	}
	for len(layers) > 2 {
		// Re-evaluate cost after every topology change, not a cached ordering.
		selected, cost := 1, blocks+1
		for i := 1; i < len(layers)-1; i++ {
			if len(layers[i+1].data) < cost {
				selected, cost = i, len(layers[i+1].data)
			}
		}
		parent, child := layers[selected], layers[selected+1]
		if err := sparse.FoldFile(child.path, parent.path, progress{}); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(child.path); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(parent.path, child.path); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(parent.path); err != nil {
			t.Fatal(err)
		}
		merged := map[int]byte{}
		for offset, value := range parent.data {
			merged[offset] = value
		}
		for offset, value := range child.data {
			merged[offset] = value
		}
		layers[selected+1].data = merged
		layers = append(layers[:selected], layers[selected+1:]...)
	}
	current, err := os.ReadFile(layers[0].path)
	if err != nil || !bytes.Equal(current, protected) {
		t.Fatalf("protected snapshot changed: %v", err)
	}
	newest, err := os.ReadFile(layers[1].path)
	if err != nil {
		t.Fatal(err)
	}
	for offset, value := range expected {
		view := protected[offset*block : (offset+1)*block]
		if _, present := layers[1].data[offset]; present {
			view = newest[offset*block : (offset+1)*block]
		}
		if !bytes.Equal(view, bytes.Repeat([]byte{value}, block)) {
			t.Fatalf("newest view changed at block %d", offset)
		}
	}
}
