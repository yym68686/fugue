package observability

import "encoding/hex"

const logBoundarySampleLimit = 4

type logBoundarySample struct {
	Digest   string `json:"sha256"`
	Required int    `json:"required"`
	Seen     int    `json:"seen"`
}

// Hashes identify the exact witness, not log completeness. Only fixed-size
// samples of missing and observed hashes are retained, never message contents.
type logBoundaryObservation struct {
	Witness               string              `json:"witness"`
	Phase                 string              `json:"phase"`
	RequiredDistinct      int                 `json:"required_distinct"`
	RequiredOccurrences   int                 `json:"required_occurrences"`
	SeenDistinct          int                 `json:"seen_distinct"`
	SeenOccurrences       int                 `json:"seen_occurrences"`
	MissingDistinct       int                 `json:"missing_distinct"`
	MissingOccurrences    int                 `json:"missing_occurrences"`
	Missing               []logBoundarySample `json:"missing_samples"`
	Seen                  []logBoundarySample `json:"seen_samples"`
	MissingSamplesOmitted int                 `json:"missing_samples_omitted"`
	SeenSamplesOmitted    int                 `json:"seen_samples_omitted"`
}

func observeLogBoundary(witness, phase string, required, seen map[[32]byte]int) *logBoundaryObservation {
	v := &logBoundaryObservation{Witness: witness, Phase: phase, RequiredDistinct: len(required), SeenDistinct: len(seen)}
	for hash, count := range required {
		v.RequiredOccurrences += count
		if observed := seen[hash]; observed < count {
			v.MissingDistinct++
			v.MissingOccurrences += count - observed
			v.Missing = insertLogBoundarySample(v.Missing, hash, count, observed)
		}
	}
	for hash, count := range seen {
		v.SeenOccurrences += count
		v.Seen = insertLogBoundarySample(v.Seen, hash, required[hash], count)
	}
	v.MissingSamplesOmitted = v.MissingDistinct - len(v.Missing)
	v.SeenSamplesOmitted = v.SeenDistinct - len(v.Seen)
	return v
}

// Keep the lexically first four digests without allocating an unbounded copy
// of an equal-timestamp boundary. Observed hashes cannot crowd out missing ones.
func insertLogBoundarySample(samples []logBoundarySample, hash [32]byte, required, seen int) []logBoundarySample {
	digest := hex.EncodeToString(hash[:])
	index := 0
	for index < len(samples) && samples[index].Digest < digest {
		index++
	}
	if index >= logBoundarySampleLimit {
		return samples
	}
	if len(samples) < logBoundarySampleLimit {
		samples = append(samples, logBoundarySample{})
	}
	copy(samples[index+1:], samples[index:len(samples)-1])
	samples[index] = logBoundarySample{Digest: digest, Required: required, Seen: seen}
	return samples
}

func cloneLogBoundaryObservation(v *logBoundaryObservation) *logBoundaryObservation {
	if v == nil {
		return nil
	}
	copy := *v
	copy.Missing = append([]logBoundarySample(nil), v.Missing...)
	copy.Seen = append([]logBoundarySample(nil), v.Seen...)
	return &copy
}
