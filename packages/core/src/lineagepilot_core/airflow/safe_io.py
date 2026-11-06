"""Reading workspace files that may be anything.

A repository can hold a symlink to a device, which reports size 0 and never ends, or to a FIFO,
which blocks forever on open. So only a regular file inside the workspace is read, and never more
than a cap.
"""

from __future__ import annotations

import os
import re
import stat

_DRIVE = re.compile(r"^[A-Za-z]:/")


def is_inside(path: str, root: str) -> bool:
    """Whether a real path is ``root`` or below it, by the platform's case rules."""
    full, base = os.path.normcase(path), os.path.normcase(root)
    return full == base or full.startswith(base.rstrip(os.sep) + os.sep)


def is_regular_file(path: str) -> bool:
    try:
        return stat.S_ISREG(os.stat(path).st_mode)
    except (OSError, ValueError):
        return False


def read_capped(path: str, cap: int, root: str | None = None) -> bytes | None:
    """The file's bytes, or None if it is not a regular file inside ``root``, or is over ``cap``."""
    real = os.path.realpath(path)
    if root is not None and not is_inside(real, os.path.realpath(root)):
        return None
    if not is_regular_file(real):
        return None
    try:
        if os.path.getsize(real) > cap:
            return None
        with open(real, "rb") as f:
            data = f.read(cap + 1)
    except OSError:
        return None
    return None if len(data) > cap else data


def to_posix(path: str) -> str:
    return path.replace(os.sep, "/")


def is_absolute(path: str) -> bool:
    """A POSIX path, or a Windows one in forward slashes such as ``C:/dags/x.py``."""
    return path.startswith("/") or _DRIVE.match(path) is not None
