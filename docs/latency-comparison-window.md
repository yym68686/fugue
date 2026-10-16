# Separate latency comparison history

Sticky canaries can receive an entire client's traffic while the stable version
has no recent requests for that operation. The final gate must not promote
without a supported comparison. Operators may configure
`gate.latency_comparison_window_seconds` independently of `gate.window_seconds`
to retain enough same-release evidence across canary steps. The default is zero,
which preserves the existing window. An explicit value must not be shorter than
the ordinary window and is bounded at one hour.

The controller first evaluates the ordinary window. Recent request minimums,
errors, absolute latency limits, readiness probes and every comparable recent
latency regression remain mandatory. Only a pure matched-sample deficit can
consult the configured longer history. That history still requires the existing
minimum on both releases in the same host/path/method/streaming cohort. A query
failure, inadequate history or regression fails closed. The release evidence
records both comparison windows; no traffic, threshold or serving artifact is
changed by introducing the code capability.
