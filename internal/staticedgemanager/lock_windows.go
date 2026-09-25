package staticedgemanager

import (
	"errors"
	"os"
)

func lockState(string) (*os.File, error) {
	return nil, errors.New("manager runs on Linux or macOS; Windows CLI supports remote management")
}
