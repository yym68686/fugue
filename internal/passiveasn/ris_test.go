package passiveasn

import (
	"context"
	"fmt"
	"net/netip"
	"os"
	"strings"
	"testing"
	"time"
)

func TestRISLookupRequiresCompleteUnambiguousPrefix(t *testing.T) {
	input4 := "% fixture\n6453\t11.0.0.0/8\t200\n6453\t11.12.0.0/16\t200\n" +
		"6453\t11.1.1.0/24\t200\n3356\t11.1.1.32/27\t1\n" +
		"6453\t11.2.1.0/24\t100\n{3356,1299}\t11.2.1.0/24\t1\n" +
		"6453\t0.0.0.0/0\t200\n3356\t12.0.0.0/25\t50\n"
	input6 := "3356\t2001:500::/32\t290\n3356\t2001:500:1::/48\t120\n3356\t2001:500:1::/56\t55\n"
	v4, err := parseRIS(strings.NewReader(input4), true, 100)
	if err != nil {
		t.Fatal(err)
	}
	v6, err := parseRIS(strings.NewReader(input6), false, 100)
	if err != nil {
		t.Fatal(err)
	}
	index := &Index{v4: v4, v6: v6}
	for prefix, want := range map[string]uint32{
		"0.0.0.0/0": 0, "11.12.1.0/24": 6453, "11.1.1.0/24": 0, "11.1.1.96/27": 6453,
		"11.2.1.0/24": 0, "12.0.0.0/24": 0, "12.0.0.0/25": 3356,
		"2001:500:1::/48": 3356, "2001:500:1:200::/56": 3356, "2001:501:1::/48": 0,
	} {
		if got := index.Lookup(netip.MustParsePrefix(prefix)); got != want {
			t.Errorf("%s ASN got %d want %d", prefix, got, want)
		}
	}
}

func TestRISParserRejectsMalformedAndExcessRows(t *testing.T) {
	for _, text := range []string{
		"12\t19.1.1.5/24\t11\n",
		"12\t2001:500::/32\t11\n",
		"{bad}\t19.1.1.0/24\t11\n",
		"12\t19.1.1.0/24\t11\n12\t19.2.1.0/24\t11\n",
	} {
		if _, err := parseRIS(strings.NewReader(text), true, 1); err == nil {
			t.Errorf("accepted %q", text)
		}
	}
}

func TestRISPublicDumpIntegration(t *testing.T) {
	if os.Getenv("FUGUE_TEST_RIS_BULK") != "1" {
		t.Skip("explicit bulk-data integration check")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	i, err := FetchRIS(ctx, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("RIS IPv4=%d IPv6=%d source=%s\n", len(i.v4), len(i.v6), i.SourceAt)
}
