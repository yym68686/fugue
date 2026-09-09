package tui

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPreferencesRoundtripPermissionsAndInvalidSettings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tui.toml")
	p := DefaultPreferences()
	p.Theme = "light"
	p.Keys = map[string]string{"refresh": "alt+r"}
	p.HiddenPanels = []string{"metrics"}
	if err := SavePreferences(path, p); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadPreferences(path)
	if err != nil || loaded.Theme != p.Theme || !loaded.panelHidden("metrics") {
		t.Fatalf("roundtrip: %+v %v", loaded, err)
	}
	info, _ := os.Stat(path)
	if info.Mode().Perm() != 0600 {
		t.Fatal("preferences not private")
	}
	p.Keys = map[string]string{"refresh": "q"}
	if p.Validate() == nil {
		t.Fatal("conflicting reserved key accepted")
	}
	p.Keys = nil
	p.Version = 99
	if p.Validate() == nil {
		t.Fatal("future version silently accepted")
	}
	os.WriteFile(path, []byte("version=1\nunknown_key=true\n"), 0600)
	if _, err := LoadPreferences(path); err == nil {
		t.Fatal("unknown option accepted")
	}
}
