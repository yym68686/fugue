package tcpproxy

import (
	"io"
	"net"
	"sync"
	"time"
)

type CopyResult struct {
	Name     string
	Bytes    int64
	Duration time.Duration
	Err      error
}

func CopyBidirectional(a, b net.Conn, aToBName, bToAName string) (CopyResult, CopyResult, string) {
	var wg sync.WaitGroup
	results := make(chan CopyResult, 2)
	copyAndClose := func(name string, dst, src net.Conn) {
		defer wg.Done()
		startedAt := time.Now()
		n, err := io.Copy(dst, src)
		CloseWrite(dst)
		results <- CopyResult{
			Name:     name,
			Bytes:    n,
			Duration: time.Since(startedAt),
			Err:      err,
		}
	}
	wg.Add(2)
	go copyAndClose(bToAName, a, b)
	go copyAndClose(aToBName, b, a)
	first := <-results
	wg.Wait()
	second := <-results
	close(results)

	aToB := first
	bToA := second
	if first.Name == bToAName {
		aToB = second
		bToA = first
	}
	return aToB, bToA, first.Name
}

func CloseWrite(conn net.Conn) {
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.CloseWrite()
		return
	}
	_ = conn.Close()
}
