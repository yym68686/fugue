package routeartifact

// A discovery hint is neither a signed packet nor authority to serve.
type HostedHint struct {
	Host     string `json:"hostname"`
	Fence    int64  `json:"fence"`
	Decision string `json:"decision"`
}
type HostedHintPage struct {
	Selections []HostedHint `json:"selections"`
	Next       string       `json:"next_after,omitempty"`
}

const HostedHintLimit = 32
