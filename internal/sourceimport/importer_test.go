package sourceimport

import (
	"reflect"
	"testing"
)

func TestGitCloneArgsIncludesRecursiveSubmodules(t *testing.T) {
	got := gitCloneArgs("https://github.com/yym68686/Cerebr", "/tmp/repo", "")
	want := []string{
		"clone",
		"--depth", "1",
		"--recurse-submodules",
		"--shallow-submodules",
		"https://github.com/yym68686/Cerebr",
		"/tmp/repo",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected clone args:\nwant: %#v\ngot:  %#v", want, got)
	}
}

func TestGitCloneArgsIncludesBranch(t *testing.T) {
	got := gitCloneArgs("https://github.com/yym68686/Cerebr", "/tmp/repo", "main")
	want := []string{
		"clone",
		"--depth", "1",
		"--recurse-submodules",
		"--shallow-submodules",
		"--branch", "main",
		"https://github.com/yym68686/Cerebr",
		"/tmp/repo",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected clone args:\nwant: %#v\ngot:  %#v", want, got)
	}
}
