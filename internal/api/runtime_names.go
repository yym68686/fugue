package api

import "fugue/internal/runtime"

func runtimeResourceName(name string) string {
	return runtime.RuntimeResourceName(name)
}
