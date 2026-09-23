# stream_gzip

A C++17 TCP server that listens on `0.0.0.0:8080`, decompresses gzip data from
each client, and writes each newline-delimited item to standard output.

Connections may contain concatenated gzip members. Member boundaries do not
act as line boundaries, so a line may start in one member and finish in the
next. A client finishes its stream by closing the connection or shutting down
its sending side. The server does not send a response.

## Build

With `VCPKG_ROOT` pointing at a vcpkg checkout:

```sh
cmake -S . -B build \
  -DCMAKE_TOOLCHAIN_FILE="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake"
cmake --build build --config Release
```

The vcpkg manifest installs Boost.Asio, Boost.Iostreams, and zlib. The server
uses a bounded worker pool sized to `std::thread::hardware_concurrency()` (and
at least one thread).

## Run and test

Run `stream_gzip` (or `stream_gzip.exe` on Windows). Output lines from a single
client retain their order; ordering between clients is intentionally
nondeterministic. Complete lines are written under a mutex so their bytes do
not interleave.

If Python 3 is available when configuring, the integration test is registered
with CTest:

```sh
ctest --test-dir build -C Release --output-on-failure
```

The test exercises empty and unterminated lines, CRLF input, concatenated
members, a line split across members, concurrent clients, malformed and
truncated gzip input, and continued service after errors.
