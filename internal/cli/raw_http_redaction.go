package cli

func sanitizeRawHTTPDiagnostic(response rawHTTPDiagnostic, redact bool) rawHTTPDiagnostic {
	if !redact {
		return response
	}
	response.Headers = redactDiagnosticHeaders(response.Headers)
	response.Body = redactDiagnosticString(response.Body)
	response.URL = redactDiagnosticString(response.URL)
	return response
}

func sanitizeAppHTTPProbe(probe appHTTPProbe, redact bool) appHTTPProbe {
	if !redact {
		return probe
	}
	probe.URL = redactDiagnosticString(probe.URL)
	probe.Error = redactDiagnosticString(probe.Error)
	probe.Headers = redactDiagnosticHeaders(probe.Headers)
	probe.Body = redactDiagnosticString(probe.Body)
	return probe
}

func sanitizeAppRequestCompareResult(result appRequestCompareResult, redact bool) appRequestCompareResult {
	if !redact {
		return result
	}
	result.Summary = redactDiagnosticString(result.Summary)
	result.Evidence = redactDiagnosticStringSlice(result.Evidence)
	result.NextActions = redactDiagnosticStringSlice(result.NextActions)
	result.Public = sanitizeAppHTTPProbe(result.Public, true)
	result.Internal = sanitizeAppHTTPProbe(result.Internal, true)
	for index := range result.EnvRequirements {
		result.EnvRequirements[index].SourceRef = redactDiagnosticString(result.EnvRequirements[index].SourceRef)
	}
	return result
}
