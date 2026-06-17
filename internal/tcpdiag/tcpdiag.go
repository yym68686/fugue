package tcpdiag

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

type Snapshot struct {
	Available       bool
	Error           string
	State           uint8
	RTTUsec         uint32
	RTTVarUsec      uint32
	MinRTTUsec      uint32
	RTOUsec         uint32
	Retransmits     uint8
	TotalRetrans    uint32
	Unacked         uint32
	Sacked          uint32
	Lost            uint32
	SegsIn          uint32
	SegsOut         uint32
	DataSegsIn      uint32
	DataSegsOut     uint32
	BytesReceived   uint64
	BytesAcked      uint64
	BytesSent       uint64
	BytesRetrans    uint64
	DeliveryRateBPS uint64
	RcvSpace        uint32
	RcvMSS          uint32
	SndMSS          uint32
	LastDataRecvMS  uint32
	TotalRTO        uint16
	TotalRTOTimeMS  uint32
}

type ProcMetric struct {
	Protocol string
	Name     string
	Value    uint64
}

func SnapshotFields(prefix string, snapshot Snapshot) map[string]any {
	prefix = strings.TrimSpace(prefix)
	if prefix != "" && !strings.HasSuffix(prefix, "_") {
		prefix += "_"
	}
	fields := map[string]any{
		prefix + "tcp_info_available": snapshot.Available,
	}
	if !snapshot.Available {
		if strings.TrimSpace(snapshot.Error) != "" {
			fields[prefix+"tcp_info_error"] = strings.TrimSpace(snapshot.Error)
		}
		return fields
	}
	fields[prefix+"tcp_state"] = snapshot.State
	fields[prefix+"tcp_rtt_us"] = snapshot.RTTUsec
	fields[prefix+"tcp_rttvar_us"] = snapshot.RTTVarUsec
	fields[prefix+"tcp_min_rtt_us"] = snapshot.MinRTTUsec
	fields[prefix+"tcp_rto_us"] = snapshot.RTOUsec
	fields[prefix+"tcp_retransmits"] = snapshot.Retransmits
	fields[prefix+"tcp_total_retrans"] = snapshot.TotalRetrans
	fields[prefix+"tcp_unacked"] = snapshot.Unacked
	fields[prefix+"tcp_sacked"] = snapshot.Sacked
	fields[prefix+"tcp_lost"] = snapshot.Lost
	fields[prefix+"tcp_segs_in"] = snapshot.SegsIn
	fields[prefix+"tcp_segs_out"] = snapshot.SegsOut
	fields[prefix+"tcp_data_segs_in"] = snapshot.DataSegsIn
	fields[prefix+"tcp_data_segs_out"] = snapshot.DataSegsOut
	fields[prefix+"tcp_bytes_received"] = snapshot.BytesReceived
	fields[prefix+"tcp_bytes_acked"] = snapshot.BytesAcked
	fields[prefix+"tcp_bytes_sent"] = snapshot.BytesSent
	fields[prefix+"tcp_bytes_retrans"] = snapshot.BytesRetrans
	fields[prefix+"tcp_delivery_rate_bps"] = snapshot.DeliveryRateBPS
	fields[prefix+"tcp_rcv_space"] = snapshot.RcvSpace
	fields[prefix+"tcp_rcv_mss"] = snapshot.RcvMSS
	fields[prefix+"tcp_snd_mss"] = snapshot.SndMSS
	fields[prefix+"tcp_last_data_recv_ms"] = snapshot.LastDataRecvMS
	fields[prefix+"tcp_total_rto"] = snapshot.TotalRTO
	fields[prefix+"tcp_total_rto_time_ms"] = snapshot.TotalRTOTimeMS
	return fields
}

func ReadProcNetTCPMetrics(snmpPath, netstatPath string) ([]ProcMetric, error) {
	var out []ProcMetric
	var combined error
	if strings.TrimSpace(snmpPath) != "" {
		metrics, err := readProcNetMetrics(snmpPath, map[string]struct{}{
			"Tcp":  {},
			"Tcp6": {},
		})
		if err != nil {
			combined = err
		}
		out = append(out, metrics...)
	}
	if strings.TrimSpace(netstatPath) != "" {
		metrics, err := readProcNetMetrics(netstatPath, map[string]struct{}{
			"TcpExt":   {},
			"IpExt":    {},
			"MPTcpExt": {},
		})
		if err != nil {
			if combined == nil {
				combined = err
			} else {
				combined = fmt.Errorf("%v; %w", combined, err)
			}
		}
		out = append(out, metrics...)
	}
	return out, combined
}

func readProcNetMetrics(path string, allowed map[string]struct{}) ([]ProcMetric, error) {
	file, err := os.Open(strings.TrimSpace(path))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var out []ProcMetric
	pending := map[string][]string{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		name, values, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		protocol := strings.TrimSpace(name)
		if _, ok := allowed[protocol]; !ok {
			continue
		}
		parts := strings.Fields(values)
		if len(parts) == 0 {
			continue
		}
		if headers, ok := pending[protocol]; ok {
			limit := len(headers)
			if len(parts) < limit {
				limit = len(parts)
			}
			for i := 0; i < limit; i++ {
				value, err := strconv.ParseUint(parts[i], 10, 64)
				if err != nil {
					continue
				}
				out = append(out, ProcMetric{
					Protocol: protocol,
					Name:     headers[i],
					Value:    value,
				})
			}
			delete(pending, protocol)
			continue
		}
		pending[protocol] = parts
	}
	if err := scanner.Err(); err != nil {
		return out, err
	}
	return out, nil
}

func ConnAddresses(conn net.Conn) (string, string) {
	if conn == nil {
		return "", ""
	}
	local := ""
	remote := ""
	if conn.LocalAddr() != nil {
		local = conn.LocalAddr().String()
	}
	if conn.RemoteAddr() != nil {
		remote = conn.RemoteAddr().String()
	}
	return local, remote
}
