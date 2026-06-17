//go:build linux

package tcpdiag

import (
	"net"
	"syscall"

	"golang.org/x/sys/unix"
)

type syscallConn interface {
	SyscallConn() (syscall.RawConn, error)
}

func SnapshotFromConn(conn net.Conn) Snapshot {
	if conn == nil {
		return Snapshot{Error: "connection is nil"}
	}
	sysConn, ok := conn.(syscallConn)
	if !ok {
		return Snapshot{Error: "connection does not expose syscall connection"}
	}
	raw, err := sysConn.SyscallConn()
	if err != nil {
		return Snapshot{Error: err.Error()}
	}
	var info *unix.TCPInfo
	var sockErr error
	if err := raw.Control(func(fd uintptr) {
		info, sockErr = unix.GetsockoptTCPInfo(int(fd), unix.IPPROTO_TCP, unix.TCP_INFO)
	}); err != nil {
		return Snapshot{Error: err.Error()}
	}
	if sockErr != nil {
		return Snapshot{Error: sockErr.Error()}
	}
	if info == nil {
		return Snapshot{Error: "tcp_info unavailable"}
	}
	return Snapshot{
		Available:       true,
		State:           info.State,
		RTTUsec:         info.Rtt,
		RTTVarUsec:      info.Rttvar,
		MinRTTUsec:      info.Min_rtt,
		RTOUsec:         info.Rto,
		Retransmits:     info.Retransmits,
		TotalRetrans:    info.Total_retrans,
		Unacked:         info.Unacked,
		Sacked:          info.Sacked,
		Lost:            info.Lost,
		SegsIn:          info.Segs_in,
		SegsOut:         info.Segs_out,
		DataSegsIn:      info.Data_segs_in,
		DataSegsOut:     info.Data_segs_out,
		BytesReceived:   info.Bytes_received,
		BytesAcked:      info.Bytes_acked,
		BytesSent:       info.Bytes_sent,
		BytesRetrans:    info.Bytes_retrans,
		DeliveryRateBPS: info.Delivery_rate,
		RcvSpace:        info.Rcv_space,
		RcvMSS:          info.Rcv_mss,
		SndMSS:          info.Snd_mss,
		LastDataRecvMS:  info.Last_data_recv,
		TotalRTO:        info.Total_rto,
		TotalRTOTimeMS:  info.Total_rto_time,
	}
}
