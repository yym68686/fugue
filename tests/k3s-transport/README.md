# K3s remote kubelet transport verification

This fixture pins the upstream revision used by the control plane and tests the
actual `pkg/daemons/control.TunnelServer.dialBackend` implementation. The tests are
copied into that upstream package; the routing function is not reimplemented.

An established remotedialer WebSocket first exchanges bytes with a separate TCP
backend. Injected EOF and timeout errors must never redirect a remote kubelet
request to the control node's loopback listener. Node-name, IPv4 and IPv6 lookup
paths are covered, along with healthy remote routing, local kubelet access,
missing sessions and the existing non-kubelet direct fallback.

The isolated CI job first requires the original code to fail exactly the six
unsafe fallback cases while its control cases pass. It then applies the pinned
patch and requires every case to pass three times with the race detector. A build
failure, skipped test, missing control, or unrelated failure is not accepted as a
successful reproduction. Evidence contains the source and patch hashes, test
events, exit codes and the patched source hash.

This is dependency verification, not a production upgrade. It does not install
K3s, publish executable artifacts or change production release intent. Full
cluster integration and a guarded native service release remain separate gates.

The patch targets upstream K3s, Copyright The K3s Authors, under the Apache 2.0
license. The source revision and origin are recorded in `source.json`.
