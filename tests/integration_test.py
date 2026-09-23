#!/usr/bin/env python3

import collections
import gzip
import socket
import subprocess
import sys
import threading
import time


HOST = "127.0.0.1"
PORT = 8080


def send(payload):
    with socket.create_connection((HOST, PORT), timeout=3) as connection:
        connection.sendall(payload)
        connection.shutdown(socket.SHUT_WR)


def wait_until_listening(process):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            raise RuntimeError(
                f"server exited during startup ({process.returncode})\n"
                f"stdout: {stdout!r}\nstderr: {stderr!r}"
            )
        try:
            with socket.create_connection((HOST, PORT), timeout=0.1) as connection:
                connection.sendall(gzip.compress(b""))
                connection.shutdown(socket.SHUT_WR)
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError("server did not listen on 127.0.0.1:8080")


def main():
    if len(sys.argv) != 2:
        raise SystemExit("usage: integration_test.py SERVER_EXECUTABLE")

    process = subprocess.Popen(
        [sys.argv[1]], stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    try:
        wait_until_listening(process)

        send(gzip.compress(b"one\n\ncrlf\r\nlast"))
        send(gzip.compress(b"joined ") + gzip.compress(b"line\n"))

        concurrent_payloads = [
            b"client-a-1\nclient-a-2\n",
            b"client-b-1\nclient-b-2\n",
        ]
        threads = [
            threading.Thread(target=send, args=(gzip.compress(payload),))
            for payload in concurrent_payloads
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        send(b"not a gzip stream")
        send(gzip.compress(b"truncated")[:-4])
        send(gzip.compress(b"after-error\n"))

        # Every complete line is flushed by the server. Allow the worker pool
        # to finish the final sessions before stopping this intentionally
        # non-terminating service.
        time.sleep(1)
    finally:
        process.terminate()
        stdout, stderr = process.communicate(timeout=5)

    actual = stdout.splitlines(keepends=True)
    expected = [
        b"one\n",
        b"\n",
        b"crlf\r\n",
        b"last\n",
        b"joined line\n",
        b"client-a-1\n",
        b"client-a-2\n",
        b"client-b-1\n",
        b"client-b-2\n",
        b"after-error\n",
    ]

    if collections.Counter(actual) != collections.Counter(expected):
        raise AssertionError(f"unexpected output\nactual: {actual!r}\nexpected: {expected!r}")

    ordered_groups = [
        [b"one\n", b"\n", b"crlf\r\n", b"last\n"],
        [b"client-a-1\n", b"client-a-2\n"],
        [b"client-b-1\n", b"client-b-2\n"],
    ]
    for group in ordered_groups:
        positions = [actual.index(line) for line in group]
        if positions != sorted(positions):
            raise AssertionError(f"per-client line order changed: {group!r} in {actual!r}")

    invalid_gzip_reports = sum(
        b"invalid gzip stream" in line.lower() for line in stderr.splitlines()
    )
    if invalid_gzip_reports < 2:
        raise AssertionError(f"malformed/truncated streams were not reported: {stderr!r}")


if __name__ == "__main__":
    main()
