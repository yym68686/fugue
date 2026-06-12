package terminal

import "io"

type GuardedWriter struct {
	Writer    io.Writer
	AllowANSI bool
}

func (w GuardedWriter) Write(p []byte) (int, error) {
	if w.Writer == nil {
		return len(p), nil
	}
	if w.AllowANSI {
		_, err := w.Writer.Write(p)
		return len(p), err
	}
	_, err := w.Writer.Write(StripANSI(p))
	return len(p), err
}

func StripANSI(input []byte) []byte {
	out := make([]byte, 0, len(input))
	for i := 0; i < len(input); i++ {
		if input[i] != 0x1b || i+1 >= len(input) || input[i+1] != '[' {
			out = append(out, input[i])
			continue
		}
		i += 2
		for i < len(input) {
			b := input[i]
			if b >= 0x40 && b <= 0x7e {
				break
			}
			i++
		}
	}
	return out
}
