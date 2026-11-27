package main

import (
	"encoding/binary"
	"errors"
	"net/netip"
)

// Linux nfnetlink wire values for one exact, read-only CT_GET.
const (
	workerCTGet       = 0x101
	workerCTReply     = 0x100
	workerNLAHeader   = 4
	workerNLHeader    = 16
	workerCTHeader    = 20
	workerNested      = 0x8000
	workerNLATypeMask = 0x3fff
)

type workerSocketTuple struct {
	source, destination netip.AddrPort
}

func workerCTQuery(tuple workerSocketTuple, sequence uint32) ([]byte, error) {
	source, destination := tuple.source.Addr().Unmap(), tuple.destination.Addr().Unmap()
	if !source.IsValid() || !destination.IsValid() || source.Zone() != "" || destination.Zone() != "" ||
		source.Is4() != destination.Is4() || tuple.source.Port() == 0 || tuple.destination.Port() == 0 || sequence == 0 {
		return nil, errors.New("invalid Front connection-tracking query")
	}
	family, sourceType, destinationType := byte(2), uint16(1), uint16(2)
	if source.Is6() {
		family, sourceType, destinationType = 10, 3, 4
	}
	ip := workerCTAttr(nil, sourceType, source.AsSlice())
	ip = workerCTAttr(ip, destinationType, destination.AsSlice())
	proto := workerCTAttr(nil, 1, []byte{6})
	proto = workerCTAttr(proto, 2, binary.BigEndian.AppendUint16(nil, tuple.source.Port()))
	proto = workerCTAttr(proto, 3, binary.BigEndian.AppendUint16(nil, tuple.destination.Port()))
	wireTuple := workerCTAttr(nil, 1|workerNested, ip)
	wireTuple = workerCTAttr(wireTuple, 2|workerNested, proto)
	request := make([]byte, workerCTHeader)
	request[workerNLHeader] = family
	request = workerCTAttr(request, 1|workerNested, wireTuple)
	binary.NativeEndian.PutUint32(request, uint32(len(request)))
	binary.NativeEndian.PutUint16(request[4:], workerCTGet)
	binary.NativeEndian.PutUint16(request[6:], 1) // NLM_F_REQUEST
	binary.NativeEndian.PutUint32(request[8:], sequence)
	return request, nil
}

func workerCTAttr(dst []byte, kind uint16, value []byte) []byte {
	length := workerNLAHeader + len(value)
	dst = binary.NativeEndian.AppendUint16(dst, uint16(length))
	dst = binary.NativeEndian.AppendUint16(dst, kind)
	dst = append(dst, value...)
	return append(dst, make([]byte, (-length)&3)...)
}

func workerCTAttrs(raw []byte) (map[uint16][]byte, error) {
	values := make(map[uint16][]byte)
	for len(raw) != 0 {
		if len(raw) < workerNLAHeader {
			return nil, errors.New("truncated conntrack attribute")
		}
		length := int(binary.NativeEndian.Uint16(raw))
		kind := binary.NativeEndian.Uint16(raw[2:]) & workerNLATypeMask
		aligned := (length + 3) &^ 3
		if length < workerNLAHeader || aligned > len(raw) || values[kind] != nil {
			return nil, errors.New("invalid or duplicate conntrack attribute")
		}
		values[kind] = raw[workerNLAHeader:length]
		raw = raw[aligned:]
	}
	return values, nil
}

func workerCTZoneZero(raw []byte) bool {
	return raw == nil || len(raw) == 2 && binary.BigEndian.Uint16(raw) == 0
}

func workerCTTuple(raw []byte, ipv4 bool) (workerSocketTuple, error) {
	v, err := workerCTAttrs(raw)
	if err != nil || !workerCTZoneZero(v[3]) {
		return workerSocketTuple{}, errors.New("invalid conntrack tuple zone")
	}
	address, ae := workerCTAttrs(v[1])
	proto, pe := workerCTAttrs(v[2])
	srcKey, dstKey := uint16(3), uint16(4)
	if ipv4 {
		srcKey, dstKey = 1, 2
	}
	src, srcOK := netip.AddrFromSlice(address[srcKey])
	dst, dstOK := netip.AddrFromSlice(address[dstKey])
	if ae != nil || pe != nil || !srcOK || !dstOK || src.Is4() != ipv4 || dst.Is4() != ipv4 || src.Is4In6() || dst.Is4In6() ||
		len(proto[1]) != 1 || proto[1][0] != 6 || len(proto[2]) != 2 || len(proto[3]) != 2 {
		return workerSocketTuple{}, errors.New("incomplete conntrack TCP tuple")
	}
	sp, dp := binary.BigEndian.Uint16(proto[2]), binary.BigEndian.Uint16(proto[3])
	if sp == 0 || dp == 0 {
		return workerSocketTuple{}, errors.New("conntrack TCP tuple has an invalid port")
	}
	return workerSocketTuple{netip.AddrPortFrom(src, sp), netip.AddrPortFrom(dst, dp)}, nil
}

func workerCTTranslation(raw []byte, sequence uint32, original workerSocketTuple, front netip.AddrPort) (netip.AddrPort, error) {
	family := byte(10)
	ipv4 := original.source.Addr().Is4()
	if ipv4 {
		family = 2
	}
	if len(raw) < workerCTHeader || len(raw) > 65536 || int(binary.NativeEndian.Uint32(raw)) != len(raw) ||
		binary.NativeEndian.Uint16(raw[4:]) != workerCTReply || binary.NativeEndian.Uint16(raw[6:])&^2 != 0 ||
		binary.NativeEndian.Uint32(raw[8:]) != sequence || raw[16] != family || raw[17] != 0 || binary.BigEndian.Uint16(raw[18:]) != 0 {
		return netip.AddrPort{}, errors.New("no exact conntrack response for held Front connection")
	}
	v, err := workerCTAttrs(raw[workerCTHeader:])
	if err != nil || !workerCTZoneZero(v[18]) {
		return netip.AddrPort{}, errors.New("invalid conntrack response attributes or zone")
	}
	got, ge := workerCTTuple(v[1], ipv4)
	reply, re := workerCTTuple(v[2], ipv4)
	proto, pe := workerCTAttrs(v[4])
	tcp, te := workerCTAttrs(proto[1])
	state := tcp[1]
	if ge != nil || re != nil || pe != nil || te != nil || len(state) != 1 || state[0] != 3 ||
		got != original || reply.source != front {
		return netip.AddrPort{}, errors.New("conntrack response does not establish the exact socket and Front listener")
	}
	return reply.destination, nil
}
