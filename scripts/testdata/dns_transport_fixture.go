package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/miekg/dns"
)

func main() {
	if len(os.Args) < 2 {
		panic("fixture mode required")
	}
	switch os.Args[1] {
	case "ready":
		if _, err := os.Stat("/tmp/ready"); err != nil {
			os.Exit(1)
		}
	case "unready":
		if err := os.Remove("/tmp/ready"); err != nil {
			panic(err)
		}
	case "restore":
		if err := os.WriteFile("/tmp/ready", []byte("ready"), 0600); err != nil {
			panic(err)
		}
	case "count":
		value, err := os.ReadFile("/tmp/count")
		if err != nil {
			panic(err)
		}
		fmt.Print(string(value))
	case "query":
		if len(os.Args) != 4 || os.Args[3] != "udp" && os.Args[3] != "tcp" {
			panic("query address and transport required")
		}
		request := new(dns.Msg)
		request.SetQuestion("transport.example.test.", dns.TypeTXT)
		client := dns.Client{Net: os.Args[3], Timeout: time.Second}
		response, _, err := client.Exchange(request, net.JoinHostPort(os.Args[2], "53"))
		result := map[string]any{"reachable": false, "transport": os.Args[3], "query_id": request.Id}
		if err == nil {
			if response.Id != request.Id || !response.Authoritative || response.Rcode != dns.RcodeSuccess || len(response.Answer) != 1 {
				panic("invalid isolated DNS answer")
			}
			answer, valid := response.Answer[0].(*dns.TXT)
			if !valid || len(answer.Txt) != 1 {
				panic("invalid isolated DNS identity")
			}
			result["reachable"], result["identity"] = true, answer.Txt[0]
		}
		if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
			panic(err)
		}
	case "serve":
		if len(os.Args) != 3 {
			panic("server identity required")
		}
		var lock sync.Mutex
		count := 0
		if err := os.WriteFile("/tmp/count", []byte("0"), 0600); err != nil {
			panic(err)
		}
		handler := dns.HandlerFunc(func(writer dns.ResponseWriter, request *dns.Msg) {
			lock.Lock()
			defer lock.Unlock()
			count++
			if err := os.WriteFile("/tmp/count", []byte(strconv.Itoa(count)), 0600); err != nil {
				panic(err)
			}
			response := new(dns.Msg)
			response.SetReply(request)
			response.Authoritative = true
			response.Answer = []dns.RR{&dns.TXT{Hdr: dns.RR_Header{Name: request.Question[0].Name, Rrtype: dns.TypeTXT, Class: dns.ClassINET, Ttl: 1}, Txt: []string{os.Args[2]}}}
			if err := writer.WriteMsg(response); err != nil {
				panic(err)
			}
		})
		started := make(chan struct{}, 2)
		for _, transport := range []string{"udp", "tcp"} {
			server := &dns.Server{Addr: ":53", Net: transport, Handler: handler, NotifyStartedFunc: func() { started <- struct{}{} }}
			go func() { panic(server.ListenAndServe()) }()
		}
		<-started
		<-started
		if err := os.WriteFile("/tmp/ready", []byte("ready"), 0600); err != nil {
			panic(err)
		}
		select {}
	default:
		panic("unknown fixture mode")
	}
}
