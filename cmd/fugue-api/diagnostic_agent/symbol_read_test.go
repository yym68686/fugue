package main

import (
	"bytes"
	"debug/elf"
	"encoding/binary"
	"io"
	"testing"
)

func benchmarkSymbolSection(b *testing.B) *elf.Section {
	b.Helper()
	const size = 64 << 20
	var header bytes.Buffer
	ident := [16]byte{0x7f, 'E', 'L', 'F', byte(elf.ELFCLASS64), byte(elf.ELFDATA2LSB), 1}
	h := elf.Header64{Ident: ident, Type: uint16(elf.ET_EXEC), Machine: uint16(elf.EM_X86_64), Version: 1, Ehsize: 64, Shoff: 64, Shentsize: 64, Shnum: 2}
	for _, v := range []any{h, elf.Section64{}, elf.Section64{Type: uint32(elf.SHT_PROGBITS), Off: 192, Size: size, Addralign: 8}} {
		if err := binary.Write(&header, binary.LittleEndian, v); err != nil {
			b.Fatal(err)
		}
	}
	data := make([]byte, 192+size)
	copy(data, header.Bytes())
	f, err := elf.NewFile(bytes.NewReader(data))
	if err != nil {
		b.Fatal(err)
	}
	return f.Sections[1]
}

func BenchmarkLargeSymbolSectionRead(b *testing.B) {
	section := benchmarkSymbolSection(b)
	for _, name := range []string{"section-data", "bounded-exact"} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(section.Size))
			for i := 0; i < b.N; i++ {
				var data []byte
				var err error
				if name == "section-data" {
					data, err = section.Data()
				} else {
					data, err = readSymbolSection(section, 80<<20)
				}
				if err != nil || len(data) != int(section.Size) {
					b.Fatal("incomplete section", err)
				}
			}
		})
	}
}

func TestBoundedSymbolSectionRejectsTruncatedInput(t *testing.T) {
	section := &elf.Section{SectionHeader: elf.SectionHeader{Name: ".gopclntab", Type: elf.SHT_NOBITS, Size: 20}}
	if _, err := readSymbolSection(section, 80<<20); err == nil || err == io.EOF {
		t.Fatalf("invalid section not rejected: %v", err)
	}
}
