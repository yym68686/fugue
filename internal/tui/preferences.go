package tui

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/pelletier/go-toml/v2"
)

type Preferences struct {
	Version       int               `toml:"version"`
	Theme         string            `toml:"theme"`
	Mouse         bool              `toml:"mouse"`
	Mode          string            `toml:"mode"`
	Window        string            `toml:"window"`
	Interval      string            `toml:"interval"`
	Graph         string            `toml:"graph"`
	DefaultScreen string            `toml:"default_screen"`
	HiddenPanels  []string          `toml:"hidden_panels,omitempty"`
	Keys          map[string]string `toml:"keys,omitempty"`
}

func DefaultPreferences() Preferences {
	return Preferences{Version: 1, Theme: "carbon", Mouse: true, Mode: "fullscreen", Window: "15m", Interval: "3s", Graph: "braille", DefaultScreen: "dashboard"}
}
func LoadPreferences(path string) (Preferences, error) {
	p := DefaultPreferences()
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return p, nil
	}
	if err != nil {
		return p, err
	}
	decoder := toml.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&p); err != nil {
		return p, fmt.Errorf("TUI preferences: %w", err)
	}
	return p, p.Validate()
}
func (p Preferences) Validate() error {
	if p.Version != 1 {
		return fmt.Errorf("unsupported TUI preference version %d", p.Version)
	}
	if p.Theme != "carbon" && p.Theme != "light" && p.Theme != "terminal" {
		return fmt.Errorf("theme must be carbon, light or terminal")
	}
	if p.Mode != "fullscreen" && p.Mode != "compact" {
		return fmt.Errorf("mode must be fullscreen or compact")
	}
	if p.Graph != "braille" && p.Graph != "block" && p.Graph != "line" && p.Graph != "ascii" {
		return fmt.Errorf("graph must be braille, block, line or ascii")
	}
	if p.Window != "5m" && p.Window != "15m" && p.Window != "1h" {
		return fmt.Errorf("window must be 5m, 15m or 1h")
	}
	interval, err := time.ParseDuration(p.Interval)
	if err != nil || interval < time.Second || interval > time.Minute {
		return fmt.Errorf("interval must be between 1s and 1m")
	}
	if !validScreen(p.DefaultScreen) {
		return fmt.Errorf("unknown default screen %q", p.DefaultScreen)
	}
	seen := map[string]bool{}
	for action, key := range p.Keys {
		if !validKeyAction(action) || key == "" || key == "ctrl+c" || seen[key] {
			return fmt.Errorf("invalid or conflicting TUI key %q for %q", key, action)
		}
		seen[key] = true
	}
	return nil
}
func SavePreferences(path string, p Preferences) error {
	if err := p.Validate(); err != nil {
		return err
	}
	data, err := toml.Marshal(p)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".tui-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}
func validScreen(s string) bool {
	switch s {
	case "dashboard", "resources", "details", "events", "logs", "tasks", "help":
		return true
	}
	return false
}
func validKeyAction(s string) bool {
	switch s {
	case "quit", "refresh", "pause", "search", "palette", "back", "copy", "theme", "window", "graph", "settings":
		return true
	}
	return false
}
