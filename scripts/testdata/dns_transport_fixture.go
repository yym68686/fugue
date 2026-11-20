package main

import (
	"encoding/json"
	"fmt"
	"io"
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
		identity, queryID, err := query(os.Args[2], os.Args[3])
		result := map[string]any{"reachable": err == nil, "transport": os.Args[3], "query_id": queryID}
		if err == nil {
			result["identity"] = identity
		}
		writeJSON(os.Stdout, result)
	case "watch":
		if len(os.Args) != 4 {
			panic("watch address and duration required")
		}
		duration, err := time.ParseDuration(os.Args[3])
		if err != nil || duration < time.Second || duration > time.Minute {
			panic("invalid bounded watch duration")
		}
		watch(os.Args[2], duration)
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

func writeJSON(output io.Writer, value any) {
	if err := json.NewEncoder(output).Encode(value); err != nil {
		panic(err)
	}
}

func query(address, transport string) (string, uint16, error) {
	request := new(dns.Msg)
	request.SetQuestion("transport.example.test.", dns.TypeTXT)
	client := dns.Client{Net: transport, Timeout: time.Second}
	response, _, err := client.Exchange(request, net.JoinHostPort(address, "53"))
	if err != nil {
		return "", request.Id, err
	}
	if response.Id != request.Id || !response.Authoritative || response.Rcode != dns.RcodeSuccess || len(response.Answer) != 1 {
		panic("invalid isolated DNS answer")
	}
	answer, valid := response.Answer[0].(*dns.TXT)
	if !valid || len(answer.Txt) != 1 {
		panic("invalid isolated DNS identity")
	}
	return answer.Txt[0], request.Id, nil
}

func watch(address string, duration time.Duration) {
	type report struct {
		Transport string         `json:"transport"`
		Answers   map[string]int `json:"answers"`
		Failures  int            `json:"failures"`
	}
	reports := make(chan report, 2)
	started := make(chan struct{}, 2)
	deadline := time.Now().Add(duration)
	for _, transport := range []string{"udp", "tcp"} {
		go func() {
			current := report{Transport: transport, Answers: make(map[string]int)}
			for first := true; time.Now().Before(deadline); first = false {
				identity, _, err := query(address, transport)
				if err != nil {
					current.Failures++
				} else {
					current.Answers[identity]++
				}
				if first {
					started <- struct{}{}
				}
				time.Sleep(50 * time.Millisecond)
			}
			reports <- current
		}()
	}
	<-started
	<-started
	writeJSON(os.Stdout, map[string]bool{"ready": true})
	writeJSON(os.Stdout, []report{<-reports, <-reports})
}
