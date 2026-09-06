from __future__ import annotations
import os, signal, subprocess, time


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
        os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def run_checked(argv, *, cwd, env, timeout, cancel=None):
    if cancel:
        cancel.check()
    proc = subprocess.Popen(
        argv,
        cwd=cwd,
        env=env,
        shell=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
        start_new_session=os.name != "nt",
    )
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
