"""Rewrite a VSIX so that the same sources always give the same bytes (security audit H4).

Usage: SOURCE_DATE_EPOCH=$(git log -1 --pretty=%ct) python scripts/normalise_vsix.py dist/lineagepilot.vsix

vsce writes each file's modification time in the build machine's time zone, the file mode the
checkout's umask gave, and the files in the order the filesystem lists them. So two builds of the
same commit hold the same files but differ in bytes, and a VSIX cannot be checked by rebuilding
it. This rewrites the archive in place with:

- the entries sorted by name, byte by byte;
- every timestamp set to SOURCE_DATE_EPOCH (the commit's time, in UTC; 1980 at the earliest, as
  zip cannot say earlier);
- mode 0644 for every file, or 0755 for one that was executable;
- no extra fields, comments or data descriptors.

Each entry's compressed bytes are copied as they are, so no file's content, CRC or size changes,
and the result does not depend on this Python's zlib. Running it twice gives the same bytes.
"""

from __future__ import annotations

import argparse
import io
import os
import struct
import sys
import tempfile
import time
import zipfile
from pathlib import Path

LOCAL = struct.Struct("<4sHHHHHIIIHH")
CENTRAL = struct.Struct("<4sHHHHHHIIIHHHHHII")
END = struct.Struct("<4sHHHHIIH")
UNIX_V20 = (3 << 8) | 20  # made by: Unix, zip specification 2.0
UTF8_NAMES = 0x0800
ENCRYPTED = 0x0001
STORED, DEFLATED = 0, 8
EARLIEST = 315532800  # 1980-01-01T00:00:00Z, the earliest time a zip entry can hold
LATEST = 4354819198  # 2107-12-31T23:59:58Z, the latest
LIMIT = 0xFFFFFFFF  # beyond this the archive needs zip64, which a VSIX never should


def dos_time(epoch: int) -> tuple[int, int]:
    """(time, date) in MS-DOS format for a Unix time, read as UTC, at two-second resolution."""
    t = time.gmtime(min(max(epoch, EARLIEST), LATEST))
    return (t.tm_hour << 11) | (t.tm_min << 5) | (t.tm_sec // 2), \
        ((t.tm_year - 1980) << 9) | (t.tm_mon << 5) | t.tm_mday


def raw_data(data: bytes, info: zipfile.ZipInfo) -> bytes:
    """The entry's compressed bytes, as stored after its local header."""
    header = LOCAL.unpack_from(data, info.header_offset)
    if header[0] != b"PK\x03\x04":
        raise ValueError(f"{info.filename}: no local header at offset {info.header_offset}")
    start = info.header_offset + LOCAL.size + header[9] + header[10]
    raw = data[start:start + info.compress_size]
    if len(raw) != info.compress_size:
        raise ValueError(f"{info.filename}: truncated")
    return raw


def normalise(data: bytes, epoch: int) -> bytes:
    """The normalised archive for `data`, a VSIX (zip) file's bytes."""
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        infos = archive.infolist()
    names = [info.filename for info in infos]
    if len(set(names)) != len(names):
        raise ValueError("duplicate entry names")
    if len(infos) >= 0xFFFF:
        raise ValueError("too many entries for a zip without zip64")
    mod_time, mod_date = dos_time(epoch)
    out = io.BytesIO()
    central = []
    for info in sorted(infos, key=lambda i: i.filename.encode("utf-8")):
        if info.flag_bits & ENCRYPTED:
            raise ValueError(f"{info.filename}: encrypted entries are not expected in a VSIX")
        if info.compress_type not in (STORED, DEFLATED):
            raise ValueError(f"{info.filename}: unexpected compression method {info.compress_type}")
        if max(info.compress_size, info.file_size, out.tell()) >= LIMIT:
            raise ValueError(f"{info.filename}: too large for a zip without zip64")
        name = info.filename.encode("utf-8")
        is_dir = info.filename.endswith("/")
        executable = (info.external_attr >> 16) & 0o111
        mode = 0o40755 if is_dir else (0o100755 if executable else 0o100644)
        external = (mode << 16) | (0x10 if is_dir else 0)
        needed = 20 if info.compress_type == DEFLATED else 10
        offset = out.tell()
        out.write(LOCAL.pack(b"PK\x03\x04", needed, UTF8_NAMES, info.compress_type, mod_time, mod_date,
                             info.CRC, info.compress_size, info.file_size, len(name), 0))
        out.write(name)
        out.write(raw_data(data, info))
        central.append(CENTRAL.pack(b"PK\x01\x02", UNIX_V20, needed, UTF8_NAMES, info.compress_type,
                                    mod_time, mod_date, info.CRC, info.compress_size, info.file_size,
                                    len(name), 0, 0, 0, 0, external, offset) + name)
    start = out.tell()
    for entry in central:
        out.write(entry)
    size = out.tell() - start
    if out.tell() >= LIMIT:
        raise ValueError("too large for a zip without zip64")
    out.write(END.pack(b"PK\x05\x06", 0, 0, len(central), len(central), size, start, 0))
    result = out.getvalue()
    check_same_files(data, result)
    return result


def check_same_files(before: bytes, after: bytes) -> None:
    """Fail unless `after` is a sound archive holding exactly the files of `before`."""
    with zipfile.ZipFile(io.BytesIO(before)) as old, zipfile.ZipFile(io.BytesIO(after)) as new:
        bad = new.testzip()
        if bad is not None:
            raise ValueError(f"{bad}: CRC check failed after rewriting")
        was = {i.filename: (i.CRC, i.file_size) for i in old.infolist()}
        now = {i.filename: (i.CRC, i.file_size) for i in new.infolist()}
        if was != now:
            raise ValueError("the rewritten archive does not hold the same files")


def source_date_epoch(value: str | None) -> int:
    if not value:
        sys.exit("set SOURCE_DATE_EPOCH (for example to `git log -1 --pretty=%ct`) "
                 "or pass --source-date-epoch")
    try:
        return int(value)
    except ValueError:
        sys.exit(f"SOURCE_DATE_EPOCH must be whole seconds since 1970, not {value!r}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("vsix", type=Path)
    parser.add_argument("--source-date-epoch", default=os.environ.get("SOURCE_DATE_EPOCH"),
                        help="the time for every entry (default: $SOURCE_DATE_EPOCH)")
    parser.add_argument("--out", type=Path, help="write here instead of rewriting the VSIX in place")
    args = parser.parse_args()
    epoch = source_date_epoch(args.source_date_epoch)
    result = normalise(args.vsix.read_bytes(), epoch)
    target = args.out or args.vsix
    with tempfile.NamedTemporaryFile(dir=target.parent, delete=False, suffix=".tmp") as tmp:
        tmp.write(result)
    os.chmod(tmp.name, 0o644)
    os.replace(tmp.name, target)
    stamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(min(max(epoch, EARLIEST), LATEST)))
    print(f"{target.name}: {len(result)} bytes, entries sorted, timestamps {stamp}")


if __name__ == "__main__":
    main()
