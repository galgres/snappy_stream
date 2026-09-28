# stream_gzip

A C++17 TCP server that listens on `0.0.0.0:8080`, decompresses either a gzip
stream or a Snappy framed stream from each client, and writes each
semicolon-delimited item to standard output. The semicolon is consumed, and
each extracted item is followed by a newline on standard output.

In Snappy mode, connections must contain the Snappy stream identifier followed
by standard compressed or uncompressed data chunks. Chunk boundaries and
repeated stream identifiers do not act as item boundaries, checksums are
validated, and standard skippable chunks are ignored. In gzip mode, each
connection carries a gzip stream.

Newlines, carriage returns, and all other non-semicolon bytes are item content.
Repeated semicolons produce empty items, while a trailing semicolon does not
produce an additional item. There is no escaping mechanism. A client finishes
its stream by closing the connection or shutting down its sending side. The
server does not send a response.

## Build

With `VCPKG_ROOT` pointing at a vcpkg checkout:

```sh
cmake -S . -B build \
  -DCMAKE_TOOLCHAIN_FILE="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake"
cmake --build build --config Release
```

The vcpkg manifest installs Boost.Asio, Boost.Iostreams, Google Snappy, and
CRC32C. The server uses a bounded worker pool sized to
`std::thread::hardware_concurrency()` (and at least one thread).

## Run and test

Run `stream_gzip` (or `stream_gzip.exe` on Windows) with the desired compression
format:

```sh
stream_gzip --compression gzip
stream_gzip --compression snappy
```

The `--compression` option defaults to `gzip` when omitted. Output items from a
single client retain their order; ordering between clients is intentionally
nondeterministic. Complete items, including their appended newlines, are
written under a mutex so their bytes do not interleave.

If Python 3 is available when configuring, the integration test is registered
with CTest:

```sh
ctest --test-dir build -C Release --output-on-failure
```

The tests exercise both command-line modes, empty and unterminated items,
embedded newlines and carriage returns, fragmented writes, concurrent clients,
malformed and truncated input, and continued service after errors. Snappy tests
also cover compressed and uncompressed chunks, repeated stream identifiers,
skippable chunks, checksum validation, and items split across chunks.
