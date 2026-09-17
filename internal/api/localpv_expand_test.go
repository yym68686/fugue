package api

import (
	"os/exec"
	"testing"
)

func TestLocalPVExpansionGuardAndResume(t *testing.T) {
	// Exercise the exact delivered program against an in-memory host, so tests
	// can never allocate disk, resize devices, or require root privileges.
	test := `
import io, types, unittest.mock as m
gib=1<<30
env={"FUGUE_NODE_UPDATE_TASK_IMAGE_PATH":"/pool/image", "FUGUE_NODE_UPDATE_TASK_VG_NAME":"pool", "FUGUE_NODE_UPDATE_TASK_EXPECTED_IMAGE_SIZE_BYTES":str(20*gib), "FUGUE_NODE_UPDATE_TASK_TARGET_IMAGE_SIZE_BYTES":str(24*gib), "FUGUE_NODE_UPDATE_TASK_DRY_RUN":"false"}
actions=[]
def command(*args):
    actions.append(args)
    if args[0]=="losetup" and "--json" in args: return '{"loopdevices":[{"name":"/dev/loop7","back-file":"/pool/image"}]}'
    if args[0]=="pvs" and "pv_name,vg_name,pv_size" in args: return '{"report":[{"pv":[{"pv_name":"/dev/loop7","vg_name":"pool","pv_size":"21474836480"}]}]}'
    if args[0]=="pvs": return '{"report":[{"pv":[{"pv_size":"25765609472"}]}]}'
    if args[0]=="blockdev": return str(24*gib)
    return ''
info=types.SimpleNamespace(st_mode=stat.S_IFREG,st_size=20*gib,st_blocks=20*gib//512,st_ino=17)
fs=types.SimpleNamespace(f_blocks=100*gib//4096,f_frsize=4096,f_bavail=30*gib//4096)
with m.patch('builtins.open',m.mock_open()), m.patch.object(fcntl,'flock'), m.patch.object(os,'fstat',return_value=info), m.patch.object(os,'stat',return_value=info), m.patch.object(os,'fstatvfs',return_value=fs), m.patch.object(os,'fsync'), m.patch.dict(expand.__globals__,run=command):
    expand(env)
    assert [a[0] for a in actions if a[0] in ('fallocate','pvresize','blockdev')]==['fallocate','blockdev','pvresize']
    # Interrupted after allocation: a retry adopts the exact target image.
    info.st_size=24*gib;info.st_blocks=24*gib//512;actions.clear();expand(env)
    assert any(a[0]=='pvresize' for a in actions)
    # Space checks fail before *any* host mutation.
    fs.f_bavail=1;actions.clear()
    try: expand(env)
    except ValueError: pass
    else: raise AssertionError('low host reserve accepted')
    assert not any(a[0] in ('fallocate','pvresize') for a in actions)
    fs.f_bavail=30*gib//4096;info.st_size=22*gib;actions.clear()
    try: expand(env)
    except ValueError: pass
    else: raise AssertionError('unexpected image size accepted')
    assert not actions
`
	code := "__name__='test'\n" + localPVExpandPython + "\n" + test
	out, err := exec.Command("python3", "-c", code).CombinedOutput()
	if err != nil {
		t.Fatalf("host growth test: %v\n%s", err, out)
	}
}
