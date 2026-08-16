package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	if len(os.Args) != 5 {
		fatalf("usage: public_dns_query <resolver> <hostname> <A|AAAA> <timeout-seconds>")
	}
	resolver := strings.TrimSpace(os.Args[1])
	hostname := strings.TrimSpace(os.Args[2])
	recordType := strings.ToUpper(strings.TrimSpace(os.Args[3]))
	timeoutSeconds, err := strconv.Atoi(strings.TrimSpace(os.Args[4]))
	if net.ParseIP(resolver) == nil || hostname == "" || timeoutSeconds <= 0 {
		fatalf("invalid resolver, hostname, or timeout")
	}
	network := "ip4"
	if recordType == "AAAA" {
		network = "ip6"
	} else if recordType != "A" {
		fatalf("record type must be A or AAAA")
	}

	timeout := time.Duration(timeoutSeconds) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	dialer := net.Dialer{Timeout: timeout}
	resolverAddress := net.JoinHostPort(resolver, "53")
	lookup := &net.Resolver{
		PreferGo:     true,
		StrictErrors: true,
		Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, network, resolverAddress)
		},
	}
	ips, err := lookup.LookupIP(ctx, network, hostname)
	if err != nil {
		fatalf("query %s %s via %s: %v", hostname, recordType, resolver, err)
	}
	for _, ip := range ips {
		fmt.Println(ip.String())
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "public_dns_query: "+format+"\n", args...)
	os.Exit(1)
}
