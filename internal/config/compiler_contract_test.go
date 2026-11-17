package config

import "testing"

func TestProductionCompilerCannotResurrectOriginalInputWriter(t *testing.T) {
	for _, value := range []string{"", "false", "0", "true"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("FUGUE_COMPILER_INPUT_OMIT_ORIGINAL", value)
			if !APIFromEnv().CompilerInputOmitOriginal {
				t.Fatal("old writer re-enabled by deployment environment")
			}
		})
	}
}
