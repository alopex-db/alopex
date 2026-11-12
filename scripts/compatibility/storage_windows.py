"""Native allocation measurement for the storage watchdog (Windows only).

Use FILE_STANDARD_INFO.AllocationSize, not logical file length or a guessed
cluster size. OPEN_REPARSE_POINT measures the link itself; inventory supplies
explicit roots for targets. Errors are fatal except a concurrent removal.
https://learn.microsoft.com/windows/win32/api/winbase/ns-winbase-file_standard_info
"""
import ctypes
import os
from ctypes import wintypes


class FileStandardInfo(ctypes.Structure):
    _fields_ = [
        ("allocation_size", ctypes.c_longlong),
        ("end_of_file", ctypes.c_longlong),
        ("number_of_links", wintypes.DWORD),
        ("delete_pending", ctypes.c_ubyte),
        ("directory", ctypes.c_ubyte),
    ]


class FileIdInfo(ctypes.Structure):
    _fields_ = [
        ("volume_serial_number", ctypes.c_ulonglong),
        ("file_id", ctypes.c_ubyte * 16),
    ]


kernel = ctypes.WinDLL("kernel32", use_last_error=True)
kernel.CreateFileW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD,
                              ctypes.c_void_p, wintypes.DWORD, wintypes.DWORD,
                              wintypes.HANDLE]
kernel.CreateFileW.restype = wintypes.HANDLE
kernel.GetFileInformationByHandleEx.argtypes = [wintypes.HANDLE, ctypes.c_int,
                                               ctypes.c_void_p, wintypes.DWORD]
kernel.GetFileInformationByHandleEx.restype = wintypes.BOOL
kernel.CloseHandle.argtypes = [wintypes.HANDLE]
kernel.CloseHandle.restype = wintypes.BOOL


def native_file_info(path):
    """Return (volume/file identity, allocated bytes) from one open handle."""
    # Zero desired access, share read/write/delete, OPEN_EXISTING. Directory
    # handles need BACKUP_SEMANTICS; OPEN_REPARSE_POINT avoids following links.
    native_path = os.path.abspath(path)
    if not native_path.startswith("\\\\?\\"):
        native_path = ("\\\\?\\UNC\\" + native_path[2:] if native_path.startswith("\\\\")
                       else "\\\\?\\" + native_path)
    handle = kernel.CreateFileW(native_path, 0, 7, None, 3, 0x02200000, None)
    if handle == wintypes.HANDLE(-1).value:
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        info = FileStandardInfo()
        if not kernel.GetFileInformationByHandleEx(handle, 1, ctypes.byref(info),
                                                   ctypes.sizeof(info)):
            raise ctypes.WinError(ctypes.get_last_error())
        if info.allocation_size < 0:
            raise RuntimeError(f"negative allocation measurement: {path}")
        identity = FileIdInfo()
        # FileIdInfo (18) supplies the full 128-bit ID; 64-bit file indexes
        # from GetFileInformationByHandle are not unique on ReFS.
        if not kernel.GetFileInformationByHandleEx(handle, 18, ctypes.byref(identity),
                                                   ctypes.sizeof(identity)):
            raise ctypes.WinError(ctypes.get_last_error())
        file_id = bytes(identity.file_id)
        if not any(file_id):
            raise RuntimeError(f"missing native file identity: {path}")
        return (identity.volume_serial_number, file_id), info.allocation_size
    finally:
        if not kernel.CloseHandle(handle):
            raise ctypes.WinError(ctypes.get_last_error())


def allocation_size(path):
    return native_file_info(path)[1]
