#!/usr/bin/env python3

import collections
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


def send_fragmented(payload, fragment_size=1):
    with socket.create_connection((HOST, PORT), timeout=3) as connection:
        for offset in range(0, len(payload), fragment_size):
            connection.sendall(payload[offset : offset + fragment_size])
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
                connection.sendall(STREAM_IDENTIFIER)
                connection.shutdown(socket.SHUT_WR)
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError("server did not listen on 127.0.0.1:8080")


def main():
    if len(sys.argv) != 2:
        raise SystemExit("usage: integration_test.py SERVER_EXECUTABLE")
    if crc32c(b"123456789") != 0xE3069283:
        raise AssertionError("CRC32C fixture implementation is invalid")

    process = subprocess.Popen(
        [sys.argv[1]], stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    try:
        wait_until_listening(process)

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

        # Every complete item is flushed by the server. Allow the worker pool
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
        b"before-corruption\n",
        b"after-error\n",
    ]

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
    for group in ordered_groups:
        positions = [actual.index(line) for line in group]
        if positions != sorted(positions):
            raise AssertionError(f"per-client item order changed: {group!r} in {actual!r}")

    invalid_snappy_reports = sum(
        b"invalid snappy stream" in line.lower() for line in stderr.splitlines()
    )
    if invalid_snappy_reports < 4:
        raise AssertionError(f"invalid streams were not reported: {stderr!r}")


if __name__ == "__main__":
    main()
