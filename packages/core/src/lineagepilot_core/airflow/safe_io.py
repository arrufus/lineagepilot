"""Reading workspace files that may be anything.

A repository can hold a symlink to a device, which reports size 0 and never ends, or to a FIFO,
which blocks forever on open. So only a regular file inside the workspace is read, and never more
than a cap.

Paths come from the repository too: a DAG file, an annotation file or a symlink names them. A path
outside the workspace is refused before the filesystem sees it, not after: on Windows
``//host/share/x`` is a UNC path, and even a ``stat`` of one can open an SMB connection to that
host, sending the user's credentials (audit LP-SEC-03). So ``resolve_inside`` checks a path as a
string first, then follows its symlinks one at a time, reading each target as a string before
following it. Nothing outside the root is passed to ``stat``, ``lstat`` or ``open``.
"""

from __future__ import annotations

import os
import re
import stat
from collections.abc import Iterator

_DRIVE = re.compile(r"^[A-Za-z]:/")
# Two separators at the start: a UNC path (//host/share), or a Windows device or long path
# (\\.\pipe\x, \\?\UNC\host). Or a drive with no separator after it (C:x), which is relative to
# that drive's current folder.
_REMOTE = re.compile(r"^(?:[\\/]{2}|\\\?\?\\|[A-Za-z]:(?![\\/]))")
# How Windows reports a symlink to a local absolute path
_LOCAL_LONG = re.compile(r"^(?:\\\\\?\\|\\\?\?\\)(?=[A-Za-z]:\\)")
# As the operating systems do: a cycle of links ends here
_MAX_LINKS = 40
# IO_REPARSE_TAG_SYMLINK and IO_REPARSE_TAG_MOUNT_POINT (a directory junction). The stat module
# defines them only on Windows.
_LINK_TAGS = (0xA000000C, 0xA0000003)
_SEPARATORS = os.sep + (os.altsep or "")


def is_inside(path: str, root: str) -> bool:
    """Whether a real path is ``root`` or below it, by the platform's case rules."""
    full, base = os.path.normcase(path), os.path.normcase(root)
    return full == base or full.startswith(base.rstrip(os.sep) + os.sep)


def is_remote(path: str) -> bool:
    """Whether a path could name another machine, a device, or a drive's current folder."""
    return _REMOTE.match(path) is not None


def is_regular_file(path: str) -> bool:
    """For a path already known to be safe to touch: see ``resolve_inside``."""
    try:
        return stat.S_ISREG(os.stat(path).st_mode)
    except (OSError, ValueError):
        return False


def _is_link(st: os.stat_result) -> bool:
    # On Windows a directory junction is not S_IFLNK, but it leads elsewhere all the same
    return stat.S_ISLNK(st.st_mode) or getattr(st, "st_reparse_tag", 0) in _LINK_TAGS


def _below(path: str, base: str) -> list[str]:
    """The components of ``path`` below ``base``, for a path inside it."""
    return [p for p in path[len(base) :].split(os.sep) if p]


def resolve_inside(path: str, root: str) -> str | None:
    """``path`` with its symlinks resolved, if it and every link it passes through stay inside
    ``root``; otherwise None. ``path`` is untrusted and absolute; ``root`` is trusted.

    A path that does not exist resolves as written, as ``os.path.realpath`` does, so a caller
    still has to check what is there.
    """
    if not isinstance(path, str) or not path or "\0" in path or is_remote(path):
        return None
    base = os.path.realpath(root)
    full = os.path.normpath(path)
    if not os.path.isabs(full) or is_remote(full) or not is_inside(full, base):
        return None
    current, pending, links = base, _below(full, base), 0
    while pending:
        name = pending.pop(0)
        if name == ".":
            continue
        if name == "..":
            current = os.path.dirname(current)
            if not is_inside(current, base):
                return None
            continue
        candidate = os.path.join(current, name)
        try:
            st = os.lstat(candidate)
        except (OSError, ValueError):
            rest = os.path.normpath(os.path.join(candidate, *pending))
            return rest if is_inside(rest, base) else None
        if not _is_link(st):
            current = candidate
            continue
        links += 1
        if links > _MAX_LINKS:
            return None
        try:
            target = _LOCAL_LONG.sub("", os.readlink(candidate))
        except (OSError, ValueError):
            return None
        if not target or "\0" in target or is_remote(target):
            return None
        drive, rest = os.path.splitdrive(target)
        if drive or (rest and rest[0] in _SEPARATORS):
            # Absolute; on Windows a link to \x is on the link's own drive
            absolute = os.path.normpath(target if drive else os.path.splitdrive(base)[0] + target)
            if not is_inside(absolute, base):
                return None
            current, pending = base, _below(absolute, base) + pending
        else:
            pending = [p for p in re.split(f"[{re.escape(_SEPARATORS)}]", target) if p] + pending
    return current


def read_capped(path: str, cap: int, root: str) -> bytes | None:
    """The file's bytes, or None if it is not a regular file inside ``root``, or is over ``cap``."""
    real = resolve_inside(path, root)
    if real is None:
        return None
    # Opened without blocking (a FIFO), and without following a link put there since
    flags = (
        os.O_RDONLY
        | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_NONBLOCK", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        if not is_regular_file(real):
            return None
        fd = os.open(real, flags)
        with os.fdopen(fd, "rb") as f:
            st = os.fstat(f.fileno())
            if not stat.S_ISREG(st.st_mode) or st.st_size > cap:
                return None
            data = f.read(cap + 1)
    except (OSError, ValueError):
        return None
    return None if len(data) > cap else data


def walk_inside(top: str, root: str) -> Iterator[tuple[str, list[str], list[str]]]:
    """``os.walk(top)``, never going into a folder that leads out of ``root``. On POSIX os.walk
    does not follow a link to a folder, but on Windows it goes into a directory junction. A
    caller may prune ``dirnames`` further, as with os.walk."""
    base = os.path.realpath(root)
    for dirpath, dirnames, filenames in os.walk(top):
        dirnames[:] = [d for d in dirnames if _stays_inside(os.path.join(dirpath, d), base)]
        yield dirpath, dirnames, filenames


def _stays_inside(path: str, base: str) -> bool:
    try:
        st = os.lstat(path)
    except (OSError, ValueError):
        return False
    return not _is_link(st) or resolve_inside(path, base) is not None


def regular_file_inside(path: str, root: str) -> str | None:
    """The real path of a regular file inside ``root``, or None (see ``resolve_inside``)."""
    real = resolve_inside(path, root)
    return real if real is not None and is_regular_file(real) else None


def to_posix(path: str) -> str:
    return path.replace(os.sep, "/")


def is_absolute(path: str) -> bool:
    """A POSIX path, or a Windows one in forward slashes such as ``C:/dags/x.py``."""
    return path.startswith("/") or _DRIVE.match(path) is not None
