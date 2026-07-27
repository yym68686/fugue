package imagecacheusage

// UsedBytesFromAvailable returns the filesystem bytes unavailable to the
// image-cache process. Filesystems may reserve part of their free blocks, so
// this value can be greater than a statfs Bfree-derived used-byte count.
func UsedBytesFromAvailable(capacityBytes, availableBytes int64) (int64, bool) {
	if capacityBytes <= 0 || availableBytes < 0 || availableBytes > capacityBytes {
		return 0, false
	}
	return capacityBytes - availableBytes, true
}

// ConservativeUsedPercent preserves a reported usage value while correcting
// it upward when capacity and process-available bytes prove that more space is
// unavailable. This keeps old image-cache reporters safe while they roll out.
func ConservativeUsedPercent(reported float64, capacityBytes, availableBytes int64) float64 {
	usedBytes, ok := UsedBytesFromAvailable(capacityBytes, availableBytes)
	if !ok {
		return reported
	}
	derived := (float64(usedBytes) / float64(capacityBytes)) * 100
	if derived > reported {
		return derived
	}
	return reported
}
