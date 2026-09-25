package entryfailover

import "errors"

func lockState(string) (func(), error) {
	return nil, errors.New("entry failover DNS mutation requires macOS or Linux for crash-safe locking")
}
