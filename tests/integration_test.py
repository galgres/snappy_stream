#!/usr/bin/env python3

import collections
import gzip
import socket
import struct
import subprocess
import sys
import threading
import time


HOST = "127.0.0.1"
PORT = 8080
STREAM_IDENTIFIER = b"\xff\x06\x00\x00sNaPpY"
CRC32C_POLYNOMIAL = 0x82F63B78
LONG_GZIP_ITEM = b"long-member-" + (b"x" * (256 * 1024))


class OutputCollector:
    def __init__(self, stream):
        self.stream = stream
        self.chunks = []
        self.condition = threading.Condition()
        self.thread = threading.Thread(target=self._read, daemon=True)
        self.thread.start()

    def _read(self):
        while True:
            chunk = self.stream.read1(8192)
            if not chunk:
                break
            with self.condition:
                self.chunks.append(chunk)
                self.condition.notify_all()

    def wait_for(self, expected, timeout=2):
        deadline = time.monotonic() + timeout
        with self.condition:
            while expected not in b"".join(self.chunks):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self.condition.wait(remaining)
            return True

    def finish(self):
        self.thread.join(timeout=5)
        if self.thread.is_alive():
            raise RuntimeError("stdout collector did not stop")
        return b"".join(self.chunks)


def crc32c(data):
    checksum = 0xFFFFFFFF
    for byte in data:
        checksum ^= byte
        for _ in range(8):
            checksum = (checksum >> 1) ^ (
                CRC32C_POLYNOMIAL if checksum & 1 else 0
            )
    return checksum ^ 0xFFFFFFFF


def masked_crc32c(data):
    checksum = crc32c(data)
    return (((checksum >> 15) | (checksum << 17)) + 0xA282EAD8) & 0xFFFFFFFF


def encode_varint(value):
    encoded = bytearray()
    while value >= 0x80:
        encoded.append((value & 0x7F) | 0x80)
        value >>= 7
    encoded.append(value)
    return bytes(encoded)


def encode_snappy_literal(data):
    encoded = bytearray(encode_varint(len(data)))
    if not data:
        return bytes(encoded)

    length_minus_one = len(data) - 1
    if len(data) <= 60:
        encoded.append(length_minus_one << 2)
    else:
        length_bytes = max(1, (length_minus_one.bit_length() + 7) // 8)
        encoded.append((59 + length_bytes) << 2)
        encoded.extend(length_minus_one.to_bytes(length_bytes, "little"))
    encoded.extend(data)
    return bytes(encoded)


def chunk(chunk_type, payload):
    if len(payload) > 0xFFFFFF:
        raise ValueError("chunk payload is too large")
    return bytes([chunk_type]) + len(payload).to_bytes(3, "little") + payload


def compressed_chunk(data):
    payload = struct.pack("<I", masked_crc32c(data)) + encode_snappy_literal(data)
    return chunk(0x00, payload)


def uncompressed_chunk(data):
    payload = struct.pack("<I", masked_crc32c(data)) + data
    return chunk(0x01, payload)


def snappy_stream(data):
    return STREAM_IDENTIFIER + compressed_chunk(data)


def send(payload):
    with socket.create_connection((HOST, PORT), timeout=3) as connection:
        connection.sendall(payload)
        connection.shutdown(socket.SHUT_WR)


def send_fragmented(payload, fragment_size=1, pause=0):
    with socket.create_connection((HOST, PORT), timeout=3) as connection:
        for offset in range(0, len(payload), fragment_size):
            connection.sendall(payload[offset : offset + fragment_size])
            if pause:
                time.sleep(pause)
        connection.shutdown(socket.SHUT_WR)


def send_with_pauses(parts, pause=0.1):
    with socket.create_connection((HOST, PORT), timeout=3) as connection:
        for index, part in enumerate(parts):
            connection.sendall(part)
            if index + 1 != len(parts):
                time.sleep(pause)
        connection.shutdown(socket.SHUT_WR)


def corrupt_gzip_checksum(payload):
    corrupted = bytearray(payload)
    corrupted[-8] ^= 0xFF
    return bytes(corrupted)


def wait_until_listening(process, probe):
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
                connection.sendall(probe)
                connection.shutdown(socket.SHUT_WR)
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError("server did not listen on 127.0.0.1:8080")


def main():
    if len(sys.argv) != 3 or sys.argv[2] not in ("gzip", "snappy", "none"):
        raise SystemExit(
            "usage: integration_test.py SERVER_EXECUTABLE {gzip|snappy|none}"
        )
    compression = sys.argv[2]
    if crc32c(b"123456789") != 0xE3069283:
        raise AssertionError("CRC32C fixture implementation is invalid")

    process = subprocess.Popen(
        [sys.argv[1], "--compression", compression],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    output = OutputCollector(process.stdout)

    try:
        if compression == "gzip":
            probe = gzip.compress(b"")
        elif compression == "snappy":
            probe = STREAM_IDENTIFIER
        else:
            probe = b""
        wait_until_listening(process, probe)

        if compression == "gzip":
            run_gzip_tests(output)
        elif compression == "snappy":
            run_snappy_tests()
        else:
            run_none_tests()

        # Every complete item is flushed by the server. Allow the worker pool
        # to finish the final sessions before stopping this intentionally
        # non-terminating service.
        time.sleep(1)
    finally:
        process.terminate()
        process.wait(timeout=5)
        stdout = output.finish()
        stderr = process.stderr.read()

    check_results(compression, stdout, stderr)


def run_gzip_tests(output):
    first_payload = b"one;;embedded\nnewline;crlf\r\ninside;last"
    send_fragmented(gzip.compress(first_payload))

    # A complete member must be processed while the connection is still open;
    # neither a following member nor socket EOF may be needed to flush it.
    first_member_item = b"available-before-second-member\n"
    with socket.create_connection((HOST, PORT), timeout=3) as connection:
        connection.sendall(gzip.compress(first_member_item[:-1] + b";"))
        if not output.wait_for(first_member_item):
            raise AssertionError(
                "completed gzip member was not emitted before later input"
            )
        connection.sendall(gzip.compress(b"second-member;"))
        connection.shutdown(socket.SHUT_WR)

    # Exercise a member split across many reads, including pauses long enough
    # for the server to observe temporary input starvation as distinct from EOF.
    send_fragmented(gzip.compress(b"bytewise-delivery;"), pause=0.002)

    # Concatenated members are one decompressed stream, whether they arrive in
    # one write or with an arbitrary pause while the connection remains open.
    send(gzip.compress(b"joined ") + gzip.compress(b"item;"))
    send_with_pauses(
        [gzip.compress(b"delayed-one;"), gzip.compress(b"delayed-two;")]
    )
    send_with_pauses(
        [gzip.compress(b"split-across-"), gzip.compress(b"members;")]
    )

    # Empty and repeated members must neither end nor reset the application
    # item stream.
    send(
        gzip.compress(b"")
        + gzip.compress(b"three-a;")
        + gzip.compress(b"three-b;")
        + gzip.compress(b"three-c;")
    )

    # The decompressed body is far larger than both socket and Boost buffers.
    send(gzip.compress(LONG_GZIP_ITEM + b";"))

    concurrent_payloads = [
        b"client-a-1;client-a-2;",
        b"client-b-1;client-b-2;",
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
    send(corrupt_gzip_checksum(gzip.compress(b"bad-checksum")))
    send(
        gzip.compress(b"before-later-corruption;")
        + corrupt_gzip_checksum(gzip.compress(b"corrupt-later"))
    )
    send(gzip.compress(b"before-malformed-member;") + b"not another member")
    send(gzip.compress(b"after-error;"))


def run_snappy_tests():
    first_stream = (
        STREAM_IDENTIFIER
        + chunk(0x80, b"ignored extension")
        + chunk(0xFE, b"padding")
        + compressed_chunk(b"one;;embedded\nnewline;crlf\r\ninside;last")
    )
    send_fragmented(first_stream)
    send(
        STREAM_IDENTIFIER
        + compressed_chunk(b"joined ")
        + STREAM_IDENTIFIER
        + uncompressed_chunk(b"item;")
    )

    concurrent_payloads = [
        b"client-a-1;client-a-2;",
        b"client-b-1;client-b-2;",
    ]
    threads = [
        threading.Thread(target=send, args=(snappy_stream(payload),))
        for payload in concurrent_payloads
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    send(b"not a snappy stream")
    send(snappy_stream(b"truncated")[:-4])

    bad_checksum = bytearray(compressed_chunk(b"bad-checksum;"))
    bad_checksum[4] ^= 0xFF
    send(
        STREAM_IDENTIFIER
        + compressed_chunk(b"before-corruption;")
        + bytes(bad_checksum)
    )
    send(STREAM_IDENTIFIER + chunk(0x02, b""))
    send(snappy_stream(b"after-error;"))


def run_none_tests():
    send_fragmented(b"one;;embedded\nnewline;crlf\r\ninside;last")
    send(b"joined item;")

    concurrent_payloads = [
        b"client-a-1;client-a-2;",
        b"client-b-1;client-b-2;",
    ]
    threads = [
        threading.Thread(target=send, args=(payload,))
        for payload in concurrent_payloads
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    send(b"after-error;")


def check_results(compression, stdout, stderr):
    actual = stdout.splitlines(keepends=True)
    expected = [
        b"one\n",
        b"\n",
        b"embedded\n",
        b"newline\n",
        b"crlf\r\n",
        b"inside\n",
        b"last\n",
        b"joined item\n",
        b"client-a-1\n",
        b"client-a-2\n",
        b"client-b-1\n",
        b"client-b-2\n",
        b"after-error\n",
    ]
    if compression == "gzip":
        expected.extend(
            [
                b"available-before-second-member\n",
                b"second-member\n",
                b"bytewise-delivery\n",
                b"delayed-one\n",
                b"delayed-two\n",
                b"split-across-members\n",
                b"three-a\n",
                b"three-b\n",
                b"three-c\n",
                LONG_GZIP_ITEM + b"\n",
                b"before-later-corruption\n",
                b"before-malformed-member\n",
            ]
        )
    elif compression == "snappy":
        expected.append(b"before-corruption\n")

    if collections.Counter(actual) != collections.Counter(expected):
        raise AssertionError(f"unexpected output\nactual: {actual!r}\nexpected: {expected!r}")

    ordered_groups = [
        [
            b"one\n",
            b"\n",
            b"embedded\n",
            b"newline\n",
            b"crlf\r\n",
            b"inside\n",
            b"last\n",
        ],
        [b"client-a-1\n", b"client-a-2\n"],
        [b"client-b-1\n", b"client-b-2\n"],
    ]
    if compression == "gzip":
        ordered_groups.extend(
            [
                [b"delayed-one\n", b"delayed-two\n"],
                [b"three-a\n", b"three-b\n", b"three-c\n"],
            ]
        )
    for group in ordered_groups:
        positions = [actual.index(line) for line in group]
        if positions != sorted(positions):
            raise AssertionError(f"per-client item order changed: {group!r} in {actual!r}")

    if compression != "none":
        invalid_reports = sum(
            f"invalid {compression} stream".encode() in line.lower()
            for line in stderr.splitlines()
        )
        minimum_reports = 4 if compression == "snappy" else 5
        if invalid_reports < minimum_reports:
            raise AssertionError(f"invalid streams were not reported: {stderr!r}")


if __name__ == "__main__":
    main()
