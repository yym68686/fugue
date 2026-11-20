package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/miekg/dns"
)

func verify() error {
	if len(os.Args) != 1 {
		return errors.New("no external endpoint permitted")
	}
	httpClient := &http.Client{Timeout: time.Second, Transport: &http.Transport{}}
	r, err := httpClient.Get("http://127.0.0.1:7834/livez")
	if err != nil {
		return err
	}
	_ = r.Body.Close()
	if r.StatusCode != 200 {
		return fmt.Errorf("livez status %d", r.StatusCode)
	}
	for _, transport := range []string{"udp", "tcp"} {
		c := dns.Client{Net: transport, Timeout: time.Second}
		q := new(dns.Msg)
		q.SetQuestion("example.test.", dns.TypeSOA)
		got, _, err := c.Exchange(q, "127.0.0.1:53")
		if err != nil || !got.Authoritative || got.Rcode != dns.RcodeSuccess || len(got.Answer) != 1 {
			return fmt.Errorf("%s did not load the synthetic global zone checkpoint: %v", transport, err)
		}
		soa, valid := got.Answer[0].(*dns.SOA)
		if !valid || soa.Serial != 7 || soa.Ns != "ns.example.test." {
			return fmt.Errorf("%s authoritative data is not from the signed checkpoint", transport)
		}
		q.SetQuestion("app.example.test.", dns.TypeA)
		got, _, err = c.Exchange(q, "127.0.0.1:53")
		if err != nil || !got.Authoritative || got.Rcode != dns.RcodeServerFailure || len(got.Answer) != 0 {
			return fmt.Errorf("%s did not fail closed without a new route proof: %v", transport, err)
		}
	}
	return nil
}

func main() {
	if err := verify(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := json.NewEncoder(os.Stdout).Encode(map[string]any{"scope": "global", "real_dns_executable": true, "authoritative_soa": true, "dynamic_fail_closed": true, "transports": []string{"udp", "tcp"}}); err != nil {
		panic(err)
	}
}
