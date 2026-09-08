package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// Small resource commands share a plain, copyable text view. JSON remains the
// complete typed payload; text never turns a missing observation into success.
func (c *CLI) renderResourceResult(payload any) error {
	if c.wantsJSON() {
		return c.writeJSON(payload)
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var result any
	if err := decoder.Decode(&result); err != nil {
		return err
	}
	result = redactEmbeddedAppState(result)
	var render func(string, any) error
	render = func(prefix string, value any) error {
		switch item := value.(type) {
		case map[string]any:
			keys := make([]string, 0, len(item))
			for k := range item {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, key := range keys {
				full := key
				if prefix != "" {
					full = prefix + "." + key
				}
				if err := render(full, item[key]); err != nil {
					return err
				}
			}
		case []any:
			if len(item) == 0 {
				_, err := fmt.Fprintf(c.stdout, "%s=[]\n", prefix)
				return err
			}
			for i, child := range item {
				if err := render(fmt.Sprintf("%s[%d]", prefix, i), child); err != nil {
					return err
				}
			}
		default:
			text := fmt.Sprint(value)
			if value == nil {
				text = "unknown"
			}
			text = strings.ReplaceAll(text, "\n", "\\n")
			_, err := fmt.Fprintf(c.stdout, "%s=%s\n", prefix, text)
			return err
		}
		return nil
	}
	return render("", result)
}
