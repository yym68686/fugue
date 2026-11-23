package platformconsumer

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/lkgcache"
	"fugue/internal/model"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformsafety"
	"fugue/internal/routeartifact"
	"fugue/internal/tlsissuance"
)

// One directory per physical executor/slot. A checkpoint is the signed
// selected configuration for one recipient, with no locally invented ACK.
// Keep expired checkpoints: expiry stops serving; it never resets the fence.
type HostedCheckpoint struct {
	Root    string
	Host    string
	Who     platformcontrol.PlatformComponentIdentityClaims
	Serving model.PlatformConsumerAssignment
	Keys    bundleauth.Keyring
}

func (c HostedCheckpoint) path() (string, error) {
	if !filepath.IsAbs(c.Root) || !tlsissuance.ValidHostname(c.Host) || c.Who.ConsumerID() == "" || c.Who.NodeID == "" || c.Who.ScopeKey != "global" || c.Serving.ArtifactKind == "" {
		return "", errors.New("invalid host checkpoint identity")
	}
	binding, _ := json.Marshal([]string{c.Host, c.Who.ConsumerID(), c.Serving.ArtifactKind})
	sum := sha256.Sum256(binding)
	return filepath.Join(c.Root, c.Host, hex.EncodeToString(sum[:])+".json"), nil
}

// Read only the durable high water for predecessor comparison. It survives
// expiry or a change of parent. Only Load with current authority may expose
// a packet as a serving input.
func (c HostedCheckpoint) read(path string) (model.PlatformArtifact, routeartifact.HostedMemberCursor, error) {
	var zero routeartifact.HostedMemberCursor
	var stored hostedWireArtifact
	raw, e := ReadFile(path, 5<<20)
	if e != nil {
		return stored.PlatformArtifact, zero, e
	}
	if e = json.Unmarshal(raw, &stored); e != nil {
		return model.PlatformArtifact{}, zero, e
	}
	a := stored.PlatformArtifact
	if a.ArtifactKind != routeartifact.HostedPacketKind || a.ID != "host-selection:"+a.ContentHash || a.GenerationSequence < 1 || a.Scope.Hostname != c.Host || !platformsafety.EvaluateArtifactIntegrity(a, c.Keys).Pass {
		return model.PlatformArtifact{}, zero, errors.New("corrupt host checkpoint")
	}
	body, e := json.Marshal(a.Content)
	var packet routeartifact.HostedSelectedPacket
	if e != nil || json.Unmarshal(body, &packet) != nil || packet.Delivery.Host != c.Host || packet.Delivery.Member != c.Who.ConsumerID() || len(packet.Delivery.Parts) == 0 || packet.Delivery.Parts[0].Recipient.Kind != c.Serving.ArtifactKind || packet.Decision != a.Generation || packet.Fence != a.GenerationSequence {
		return model.PlatformArtifact{}, zero, errors.New("foreign host checkpoint")
	}
	return a, routeartifact.HostedMemberCursor{Host: c.Host, Member: c.Who.ConsumerID(), Kind: c.Serving.ArtifactKind, Fence: packet.Fence, Decision: packet.Decision}, nil
}

func (c HostedCheckpoint) Load(now time.Time) (routeartifact.HostedSelectedPacket, error) {
	path, e := c.path()
	if e != nil {
		return routeartifact.HostedSelectedPacket{}, e
	}
	a, cur, e := c.read(path)
	if e != nil {
		return routeartifact.HostedSelectedPacket{}, e
	}
	return routeartifact.VerifyHostedPacket(a, c.Host, c.Who, c.Serving, cur.Fence, cur.Decision, c.Keys, now)
}

// Reserved reads historical configuration for key reservations and restart
// predecessor comparison ONLY. Verify even an expired packet at its original
// selection instant; never return it as current serving or readiness evidence.
func (c HostedCheckpoint) Reserved() (routeartifact.HostedSelectedPacket, error) {
	var z routeartifact.HostedSelectedPacket
	path, err := c.path()
	if err != nil {
		return z, err
	}
	files, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		return z, err
	}
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".json") && file.Name() != filepath.Base(path) {
			return z, errors.New("foreign DNS checkpoint in reserved directory")
		}
	}
	a, cur, err := c.read(path)
	if err != nil {
		return z, err
	}
	raw, err := json.Marshal(a.Content)
	var p routeartifact.HostedSelectedPacket
	if err != nil || json.Unmarshal(raw, &p) != nil {
		return z, errors.New("invalid historical packet")
	}
	// The platform signature canonicalizes envelope dates to microseconds.
	// The signed selection body may carry a finer clock. Match the envelope
	// at its precision, then replay at the actual signed selection instant.
	if !p.SelectedAt.UTC().Truncate(time.Microsecond).Equal(a.CreatedAt) {
		return z, errors.New("historical selection and envelope timestamps disagree")
	}
	sel := model.PlatformConsumerAssignment{ArtifactKind: c.Serving.ArtifactKind, ScopeKey: c.Who.ScopeKey, ReleaseChannel: "full", ReleaseSetID: p.Parent, ArtifactID: p.Child, ContentHash: p.ChildDigest, ArtifactReleaseID: p.Delivery.MembershipRelease, ExpectedConsumerSetID: p.Delivery.MembershipSet, Revision: p.Delivery.MembershipRevision, FencingToken: p.Delivery.MembershipFence}
	return routeartifact.VerifyHostedPacket(a, c.Host, c.Who, sel, cur.Fence, cur.Decision, c.Keys, p.SelectedAt)
}

// Call with the executor's live snapshot locked. Different hosts use
// different checkpoint files and locks. Write the full selection before
// changing the in-memory serving pointer. On persistence error reload the
// checkpoint before another transition, since rename may already have won.
func (c HostedCheckpoint) Commit(a model.PlatformArtifact, live, base map[routeartifact.HostedRecipient]routeartifact.HostedRecipientInput, now time.Time) (routeartifact.HostedMemberChange, error) {
	var zero routeartifact.HostedMemberChange
	path, e := c.path()
	if e != nil {
		return zero, e
	}
	if e = os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		return zero, e
	}
	lock, e := os.OpenFile(strings.TrimSuffix(path, ".json")+".lock", os.O_RDWR|os.O_CREATE, 0600)
	if e != nil {
		return zero, e
	}
	defer lock.Close()
	if e = lockHostedCheckpoint(lock); e != nil {
		return zero, e
	}
	old, cur, e := c.read(path)
	if e != nil && !errors.Is(e, os.ErrNotExist) {
		return zero, e
	}
	change, e := routeartifact.PlanHostedMemberChange(a, c.Host, c.Who, c.Serving, cur, live, base, c.Keys, now)
	if e != nil {
		return zero, e
	}
	if change.Next == cur {
		if old.ID != a.ID {
			return zero, errors.New("different packet for the same checkpoint")
		}
	} else {
		raw, e := json.Marshal(a)
		if e != nil || len(raw) > 5<<20 {
			return zero, errors.New("unrepresentable host checkpoint")
		}
		if e = lkgcache.AtomicWriteFile(path, raw, 0600); e != nil {
			return zero, e
		}
	}
	// Even an idempotent restart may follow a crash (or I/O error)
	// between rename and the directory sync.
	for _, name := range []string{filepath.Dir(path), c.Root, filepath.Dir(c.Root)} {
		dir, err := os.Open(name)
		if err != nil {
			return zero, err
		}
		err = dir.Sync()
		dir.Close()
		if err != nil {
			return zero, err
		}
	}
	return change, nil
}

// Names are startup tombstones, not authority to use any saved content.
// Even a different enrollment must not silently serve an older global
// route while fetching its own assignment. Read the index before listeners.
func HostedCheckpointHosts(root string) ([]string, error) {
	if !filepath.IsAbs(root) {
		return nil, errors.New("invalid host checkpoint root")
	}
	d, err := os.Open(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer d.Close()
	entries, err := d.ReadDir(8193)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if len(entries) > 8192 {
		return nil, errors.New("too many local host checkpoints")
	}
	var hosts []string
	for _, item := range entries {
		if !item.IsDir() || !tlsissuance.ValidHostname(item.Name()) {
			return nil, errors.New("unrecognized host checkpoint entry")
		}
		// Even an interrupted first write reserves this host; other hosts
		// need not wait for its revalidation.
		hosts = append(hosts, item.Name())
	}
	return hosts, nil
}
