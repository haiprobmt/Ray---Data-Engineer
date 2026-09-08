from __future__ import annotations
import os, signal, subprocess, time
from collections import deque
from threading import Thread


def diagnostic_line(line):
    """Keep bounded engineering diagnostics, never credential-bearing lines/URLs."""
    import re
    from .memory import SECRET, SECRET_KEY, redact
    if SECRET.search(line) or re.search(SECRET_KEY, line, re.I):
        return "[credential-bearing diagnostic omitted]"
    line = re.sub(r"\x1b\[[0-9;]*[A-Za-z]", "", line)
    line = re.sub(r"(?:https?|postgres(?:ql)?|mssql)://\S+", "[URL omitted]", line)
    line = re.sub(r"\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+", "[token omitted]", line)
    return redact(line).rstrip()


def collect_diagnostics(stream, lines):
    # Drain both pipes concurrently. Drop oversized lines in their entirety so a
    # truncated prefix cannot separate a credential value from its field name.
    try:
        while chunk := stream.readline(4097):
            if len(chunk) > 4096:
                while chunk and not chunk.endswith(b"\n"):
                    chunk = stream.readline(4097)
                lines.append("[oversized diagnostic line omitted]")
                continue
            lines.append(diagnostic_line(chunk.decode("utf-8", errors="replace")))
    finally:
        stream.close()


def terminate_tree(process):
    if process.poll() is not None:
        return
    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/PID", str(process.pid), "/T", "/F"],
            capture_output=True,
            timeout=15,
            check=False,
        )
    else:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def run_checked(argv, *, cwd, env, timeout, cancel=None, diagnostics=None):
    if cancel:
        cancel.check()
    proc = subprocess.Popen(
        argv,
        cwd=cwd,
        env=env,
        shell=False,
        stdout=subprocess.PIPE if diagnostics is not None else subprocess.DEVNULL,
        stderr=subprocess.PIPE if diagnostics is not None else subprocess.DEVNULL,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
        start_new_session=os.name != "nt",
    )
    readers = []
    buffers = []
    if diagnostics is not None:
        for stream in (proc.stdout, proc.stderr):
            lines = deque(maxlen=24)
            reader = Thread(target=collect_diagnostics, args=(stream, lines), daemon=True)
            readers.append(reader)
            buffers.append(lines)
            reader.start()
    deadline = time.monotonic() + timeout
    try:
        while proc.poll() is None:
            if cancel:
                cancel.check()
            if time.monotonic() > deadline:
                raise TimeoutError("Validation timed out")
            time.sleep(0.1)
        if cancel:
            cancel.check()
        return proc.returncode
    finally:
        terminate_tree(proc)
        for reader in readers:
            reader.join(timeout=1)
        if diagnostics is not None:
            # Only complete sanitized lines; bound the total delivered to the model.
            used = 0
            for lines in reversed(buffers):
                for line in list(lines):
                    if used + len(line) <= 12000:
                        diagnostics.append(line)
                        used += len(line)
