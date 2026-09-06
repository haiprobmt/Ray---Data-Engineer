"""Windows user-bound DPAPI secret storage; secrets never enter project files."""

import ctypes, os
from ctypes import wintypes
from pathlib import Path


class Blob(ctypes.Structure):
    _fields_ = [("length", wintypes.DWORD), ("data", ctypes.POINTER(ctypes.c_ubyte))]


def _crypt(data, decrypt=False):
    if os.name != "nt":
        raise RuntimeError(
            "Use RAY_TELEGRAM_BOT_TOKEN on this OS; local protected storage requires Windows"
        )
    source = ctypes.create_string_buffer(data)
    blob = Blob(len(data), ctypes.cast(source, ctypes.POINTER(ctypes.c_ubyte)))
    out = Blob()
    fn = (
        ctypes.windll.crypt32.CryptUnprotectData
        if decrypt
        else ctypes.windll.crypt32.CryptProtectData
    )
    ok = fn(ctypes.byref(blob), None, None, None, None, 1, ctypes.byref(out))
    if not ok:
        raise RuntimeError("Windows could not access this user-bound secret")
    try:
        return ctypes.string_at(out.data, out.length)
    finally:
        ctypes.windll.kernel32.LocalFree.argtypes = [ctypes.c_void_p]
        ctypes.windll.kernel32.LocalFree(out.data)


def save_token(data_dir, token):
    import re

    if not re.fullmatch(r"\d{6,}:[A-Za-z0-9_-]{25,}", token):
        raise ValueError("Invalid Telegram bot token format")
    directory = Path(data_dir) / "secrets"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "telegram.dpapi").write_bytes(_crypt(token.encode()))


def load_token(data_dir):
    token = os.environ.get("RAY_TELEGRAM_BOT_TOKEN")
    if token:
        return token
    path = Path(data_dir) / "secrets" / "telegram.dpapi"
    if not path.exists():
        raise RuntimeError(
            "Set the bot token locally using ray secret-set or RAY_TELEGRAM_BOT_TOKEN"
        )
    return _crypt(path.read_bytes(), True).decode()
