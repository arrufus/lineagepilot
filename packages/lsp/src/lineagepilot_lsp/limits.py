"""A memory limit for the child processes that read the project (audit LP-SEC-05).

The indexer, the DAG scan and the baseline reader parse files from the workspace, which is
untrusted. Whatever they cost, they must not take the machine with them:

- On Linux the child limits itself (``apply_in_child``) with ``RLIMIT_AS``: an allocation past it
  raises ``MemoryError``.
- On every platform the server watches the child's memory (``resident_bytes``) and kills it past
  the limit: the only limit on macOS, which does not enforce ``RLIMIT_AS``, and on Windows. A Job
  Object would limit a Windows child too, but a child in one made the server's end-to-end tests
  hang on Windows CI when it was killed, so the server's watch is used there instead.

The limit scales with the artifact size limit, which the user sets and an untrusted workspace
cannot raise: parsing a manifest takes many times its size (spike S2: about 1 GB at 5,000
models). It never exceeds most of the machine's memory.
"""

from __future__ import annotations

import ctypes
import logging
import os
import sys
from typing import Any

logger = logging.getLogger(__name__)

MIN_LIMIT = 4 * 2**30
# Bytes of memory allowed per byte of the artifact size limit
PER_ARTIFACT_BYTE = 16
# Of the machine's physical memory
MAX_SHARE = 0.75


def physical_memory() -> int | None:
    try:
        if sys.platform == "win32":
            return _windows_physical_memory()
        return os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")
    except (ValueError, OSError, AttributeError):
        return None


def memory_limit(max_artifact_bytes: int) -> int:
    """The memory a child may use when artifacts can be ``max_artifact_bytes`` large."""
    limit = max(MIN_LIMIT, PER_ARTIFACT_BYTE * max_artifact_bytes)
    total = physical_memory()
    return min(limit, int(total * MAX_SHARE)) if total else limit


def apply_in_child(limit: int) -> str | None:
    """Limit this process's memory, and say how; None where only the server's watch applies."""
    try:
        if sys.platform.startswith("linux"):
            import resource

            _, hard = resource.getrlimit(resource.RLIMIT_AS)
            soft = limit if hard == resource.RLIM_INFINITY else min(limit, hard)
            resource.setrlimit(resource.RLIMIT_AS, (soft, hard))
            return "RLIMIT_AS"
    except (OSError, ValueError) as e:
        logger.warning("The memory limit could not be set: %s", e)
    return None


def resident_bytes(pid: int) -> int | None:
    """A process's resident memory, where it can be read cheaply; None elsewhere."""
    try:
        if sys.platform.startswith("linux"):
            with open(f"/proc/{pid}/statm", "rb") as f:
                return int(f.read().split()[1]) * os.sysconf("SC_PAGE_SIZE")
        if sys.platform == "darwin":
            return _darwin_resident(pid)
        if sys.platform == "win32":
            return _windows_private_bytes(pid)
    except (OSError, ValueError, IndexError, AttributeError):
        return None
    return None


# macOS: proc_pidinfo(PROC_PIDTASKINFO), whose struct proc_taskinfo starts with the virtual and
# the resident size, and is 96 bytes in all
_PROC_PIDTASKINFO = 4
_TASKINFO_SIZE = 96
_libproc: Any = None


def _darwin_resident(pid: int) -> int | None:
    global _libproc
    if _libproc is None:
        _libproc = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
        _libproc.proc_pidinfo.argtypes = [
            ctypes.c_int,
            ctypes.c_int,
            ctypes.c_uint64,
            ctypes.c_void_p,
            ctypes.c_int,
        ]
        _libproc.proc_pidinfo.restype = ctypes.c_int
    info = (ctypes.c_uint64 * (_TASKINFO_SIZE // 8))()
    got = _libproc.proc_pidinfo(pid, _PROC_PIDTASKINFO, 0, info, _TASKINFO_SIZE)
    return int(info[1]) if got == _TASKINFO_SIZE else None


# Windows ---------------------------------------------------------------------------------------
_PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
_PROCESS_VM_READ = 0x0010


def _windows_private_bytes(pid: int) -> int | None:
    """A process's private memory (what it has committed), from GetProcessMemoryInfo."""
    from ctypes import wintypes

    class Counters(ctypes.Structure):
        _fields_ = [
            ("cb", wintypes.DWORD),
            ("PageFaultCount", wintypes.DWORD),
            ("PeakWorkingSetSize", ctypes.c_size_t),
            ("WorkingSetSize", ctypes.c_size_t),
            ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
            ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
            ("PagefileUsage", ctypes.c_size_t),
            ("PeakPagefileUsage", ctypes.c_size_t),
        ]

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)  # type: ignore[attr-defined]
    kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.K32GetProcessMemoryInfo.argtypes = [wintypes.HANDLE, ctypes.c_void_p, wintypes.DWORD]
    kernel32.K32GetProcessMemoryInfo.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    handle = kernel32.OpenProcess(_PROCESS_QUERY_LIMITED_INFORMATION | _PROCESS_VM_READ, False, pid)
    if not handle:
        return None
    try:
        counters = Counters()
        counters.cb = ctypes.sizeof(counters)
        if not kernel32.K32GetProcessMemoryInfo(handle, ctypes.byref(counters), counters.cb):
            return None
        return int(counters.PagefileUsage)
    finally:
        kernel32.CloseHandle(handle)


def _windows_physical_memory() -> int | None:
    class MemoryStatus(ctypes.Structure):
        _fields_ = [
            ("dwLength", ctypes.c_uint32),
            ("dwMemoryLoad", ctypes.c_uint32),
            ("ullTotalPhys", ctypes.c_uint64),
            ("ullAvailPhys", ctypes.c_uint64),
            ("ullTotalPageFile", ctypes.c_uint64),
            ("ullAvailPageFile", ctypes.c_uint64),
            ("ullTotalVirtual", ctypes.c_uint64),
            ("ullAvailVirtual", ctypes.c_uint64),
            ("ullAvailExtendedVirtual", ctypes.c_uint64),
        ]

    status = MemoryStatus()
    status.dwLength = ctypes.sizeof(status)
    kernel32 = ctypes.WinDLL("kernel32")  # type: ignore[attr-defined]
    return int(status.ullTotalPhys) if kernel32.GlobalMemoryStatusEx(ctypes.byref(status)) else None
