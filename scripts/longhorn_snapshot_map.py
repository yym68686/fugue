"""Read sparse metadata only; usable inside an existing replica instance manager.

Never opens the live head, reads file contents, syncs, punches holes or changes
files. FIEMAP flags are zero. The result describes allocation, not a GC permit.
"""
import fcntl
import json
import os
import re
import stat
import struct
import sys
import time


def intersection(left, right):
    i = j = size = 0
    while i < len(left) and j < len(right):
        a, b = left[i]
        c, d = right[j]
        size += max(0, min(b, d) - max(a, c))
        if b < d:
            i += 1
        else:
            j += 1
    return size


def identity(info):
    return [info.st_dev, info.st_ino, info.st_size, info.st_blocks,
            info.st_mtime_ns, info.st_ctime_ns]


def filesystem(directory):
    """Resolve the actual mount, not a path-name or fs magic heuristic."""
    selected = None
    with open('/proc/self/mountinfo') as mounts:
        for line in mounts:
            fields = line.split()
            separator = fields.index('-')
            mount = re.sub(r'\\([0-7]{3})', lambda match: chr(int(match[1], 8)), fields[4])
            if directory == mount or directory.startswith(mount.rstrip('/') + '/'):
                if selected is None or len(mount) > len(selected['mount']):
                    selected = dict(mount=mount, type=fields[separator + 1], device=fields[2],
                                    read_only='ro' in fields[5].split(','))
    if selected is None:
        raise ValueError('replica filesystem is unknown')
    return selected


def inspect(request):
    required = {'directory', 'parent', 'child'}
    if set(request) not in (required, required | {'growthSampleSeconds'}):
        raise ValueError('invalid sparse observation request')
    directory = request['directory']
    if not re.fullmatch(r'/host/[-a-zA-Z0-9_/]+/replicas/[-a-z0-9]+', directory):
        raise ValueError('invalid replica directory')
    if os.path.realpath(directory) != directory or not stat.S_ISDIR(os.stat(directory).st_mode):
        raise ValueError('replica directory identity changed')
    sample = request.get('growthSampleSeconds', 0)
    if type(sample) is not int or sample not in (0, 2, 3, 4, 5):
        raise ValueError('invalid filesystem growth sample')
    mount = filesystem(directory) if sample else None
    if mount and (mount['type'] != 'ext4' or mount['read_only']):
        raise ValueError('fixed-capacity recovery requires writable ext4')
    device = os.stat(directory).st_dev
    if mount and mount['device'] != str(os.major(device)) + ':' + str(os.minor(device)):
        raise ValueError('replica mount device changed')
    initial, started = os.statvfs(directory), time.monotonic()
    names = [request['parent'], request['child']]
    if len(set(names)) != 2 or any(not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,179}', n) or n == 'volume-head' for n in names):
        raise ValueError('two immutable snapshot identities are required')
    deadline = time.monotonic() + 12
    records, ranges = [], []
    for name in names:
        fd = os.open(directory + '/volume-snap-' + name + '.img', os.O_RDONLY | os.O_NOFOLLOW)
        try:
            before = os.fstat(fd)
            if not stat.S_ISREG(before.st_mode) or before.st_dev != device or before.st_nlink != 1:
                raise ValueError('snapshot is not a regular file')
            current, offset = [], 0
            # At most 131072 extents per file and 12 seconds for both files.
            for _ in range(512):
                if time.monotonic() >= deadline:
                    raise ValueError('sparse metadata time budget exceeded')
                buffer = bytearray(32 + 256 * 56)
                struct.pack_into('<QQIIII', buffer, 0, offset, before.st_size - offset, 0, 0, 256, 0)
                fcntl.ioctl(fd, 0xC020660B, buffer, True)
                mapped = struct.unpack_from('<QQIIII', buffer, 0)[3]
                if mapped > 256:
                    raise ValueError('invalid FIEMAP response')
                if not mapped:
                    break
                last = False
                for index in range(mapped):
                    fields = struct.unpack_from('<QQQQQIIII', buffer, 32 + index * 56)
                    start, length, flags = fields[0], fields[2], fields[5]
                    # Unknown, delayed, encoded, encrypted, unaligned, inline,
                    # tail packed and reflinked data require another planner.
                    if flags & (2 | 4 | 8 | 128 | 256 | 512 | 1024 | 8192):
                        raise ValueError('unsupported FIEMAP allocation flags')
                    if length <= 0 or start < offset or start + length > before.st_size or (current and start < current[-1][1]):
                        raise ValueError('invalid FIEMAP extent ordering')
                    current.append((start, start + length))
                    last |= bool(flags & 1)
                offset = current[-1][1]
                if last or offset == before.st_size:
                    break
            else:
                raise ValueError('sparse metadata extent budget exceeded')
            if identity(before) != identity(os.fstat(fd)):
                raise ValueError('snapshot changed during sparse observation')
            records.append(dict(name=name, identity=identity(before),
                                allocated_bytes=before.st_blocks * 512,
                                mapped_bytes=sum(b - a for a, b in current),
                                extent_count=len(current)))
            ranges.append(current)
        finally:
            os.close(fd)
    overlap = intersection(*ranges)
    if sample:
        time.sleep(max(0, sample - (time.monotonic() - started)))
        if filesystem(directory) != mount or os.stat(directory).st_dev != device:
            raise ValueError('replica filesystem changed during observation')
        for record in records:
            if identity(os.lstat(directory + '/volume-snap-' + record['name'] + '.img')) != record['identity']:
                raise ValueError('snapshot changed during growth sample')
    fs = os.statvfs(directory)
    result = dict(files=records, overlap_bytes=overlap,
                # Longhorn v1.12.1 FoldFile(child,parent) overwrites all child
                # data in the parent, then hardlinks the merged inode as child.
                fold_write_bytes=records[1]['mapped_bytes'],
                new_mapped_bytes=records[1]['mapped_bytes'] - overlap,
                physical_free_bytes=fs.f_bfree * fs.f_frsize,
                physical_available_bytes=fs.f_bavail * fs.f_frsize,
                physical_total_bytes=fs.f_blocks * fs.f_frsize,
                content_bytes_read=0, sync_requested=False)
    if sample:
        result.update(filesystem=mount, filesystem_device=device,
                      growth_sample_seconds=time.monotonic() - started,
                      observed_growth_bytes=max(0, initial.f_bavail * initial.f_frsize - fs.f_bavail * fs.f_frsize))
    return result


if __name__ == '__main__':
    try:
        raw = sys.stdin.read(4097)
        if len(raw) > 4096:
            raise ValueError('observation request exceeds budget')
        print(json.dumps(inspect(json.loads(raw)), separators=(',', ':')))
    except (ValueError, OSError, KeyError, TypeError):
        # Never print path, raw request, contents, or provider exception text.
        print('bounded sparse metadata observation unavailable', file=sys.stderr)
        sys.exit(1)
