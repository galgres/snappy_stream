#include <boost/asio.hpp>
#include <boost/iostreams/categories.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/operations.hpp>
#include <crc32c/crc32c.h>
#include <snappy.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <exception>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#endif

namespace {

using boost::asio::ip::tcp;

constexpr unsigned short kPort = 8080;
constexpr char kDelimiter = ';';
constexpr std::size_t kMaxSnappyChunkSize = 65536;
constexpr std::uint8_t kCompressedChunk = 0x00;
constexpr std::uint8_t kUncompressedChunk = 0x01;
constexpr std::uint8_t kStreamIdentifierChunk = 0xff;
constexpr std::uint8_t kFirstSkippableChunk = 0x80;
constexpr std::array<char, 6> kStreamIdentifier = {'s', 'N', 'a', 'P', 'p', 'Y'};

std::mutex output_mutex;
std::mutex error_mutex;

void print_item(const std::string& item) {
    const std::lock_guard<std::mutex> lock(output_mutex);
    std::cout.write(item.data(), static_cast<std::streamsize>(item.size()));
    std::cout.put('\n');
    std::cout.flush();
}

void report_error(const std::string& peer, const std::string& message) {
    const std::lock_guard<std::mutex> lock(error_mutex);
    std::cerr << "client " << peer << ": " << message << '\n';
}

std::string peer_name(tcp::socket& socket) {
    boost::system::error_code error;
    const tcp::endpoint endpoint = socket.remote_endpoint(error);
    if (error) {
        return "<unknown>";
    }

    std::ostringstream name;
    name << endpoint.address().to_string() << ':' << endpoint.port();
    return name.str();
}

class socket_source {
public:
    using char_type = char;
    using category = boost::iostreams::source_tag;

    explicit socket_source(std::shared_ptr<tcp::socket> socket)
        : socket_(std::move(socket)) {}

    std::streamsize read(char* destination, std::streamsize size) {
        if (size <= 0) {
            return 0;
        }

        boost::system::error_code error;
        const std::size_t count = socket_->read_some(
            boost::asio::buffer(destination, static_cast<std::size_t>(size)),
            error);

        if (error == boost::asio::error::eof) {
            return -1;
        }
        if (error) {
            throw boost::system::system_error(error);
        }

        return static_cast<std::streamsize>(count);
    }

private:
    std::shared_ptr<tcp::socket> socket_;
};

class snappy_stream_error : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

std::uint32_t load_little_endian_32(const char* bytes) {
    return static_cast<std::uint32_t>(static_cast<unsigned char>(bytes[0])) |
           (static_cast<std::uint32_t>(static_cast<unsigned char>(bytes[1])) << 8U) |
           (static_cast<std::uint32_t>(static_cast<unsigned char>(bytes[2])) << 16U) |
           (static_cast<std::uint32_t>(static_cast<unsigned char>(bytes[3])) << 24U);
}

std::size_t load_little_endian_24(const char* bytes) {
    return static_cast<std::size_t>(static_cast<unsigned char>(bytes[0])) |
           (static_cast<std::size_t>(static_cast<unsigned char>(bytes[1])) << 8U) |
           (static_cast<std::size_t>(static_cast<unsigned char>(bytes[2])) << 16U);
}

std::uint32_t mask_crc32c(std::uint32_t checksum) {
    return ((checksum >> 15U) | (checksum << 17U)) + 0xa282ead8U;
}

class snappy_framed_decompressor {
public:
    using char_type = char;
    using category = boost::iostreams::multichar_input_filter_tag;

    template <typename Source>
    std::streamsize read(Source& source, char* destination, std::streamsize size) {
        if (size <= 0) {
            return 0;
        }

        std::streamsize written = 0;
        while (written < size) {
            if (output_offset_ < output_.size()) {
                const std::size_t requested = static_cast<std::size_t>(size - written);
                const std::size_t available = output_.size() - output_offset_;
                const std::size_t count = std::min(requested, available);
                std::memcpy(destination + written, output_.data() + output_offset_, count);
                output_offset_ += count;
                written += static_cast<std::streamsize>(count);
                if (output_offset_ == output_.size()) {
                    break;
                }
                continue;
            }

            if (at_end_) {
                break;
            }

            read_next_chunk(source);
        }

        return written == 0 ? -1 : written;
    }

private:
    template <typename Source>
    bool read_exact(Source& source, char* destination, std::size_t size, bool allow_clean_eof) {
        std::size_t total = 0;
        while (total < size) {
            const std::streamsize count = boost::iostreams::read(
                source,
                destination + total,
                static_cast<std::streamsize>(size - total));

            if (count == -1) {
                if (total == 0 && allow_clean_eof) {
                    return false;
                }
                throw snappy_stream_error("truncated chunk");
            }
            if (count == 0) {
                throw snappy_stream_error("source returned no data");
            }

            total += static_cast<std::size_t>(count);
        }
        return true;
    }

    template <typename Source>
    void skip_exact(Source& source, std::size_t size) {
        std::array<char, 8192> scratch{};
        while (size > 0) {
            const std::size_t count = std::min(size, scratch.size());
            read_exact(source, scratch.data(), count, false);
            size -= count;
        }
    }

    template <typename Source>
    void read_next_chunk(Source& source) {
        std::array<char, 4> header{};
        if (!read_exact(source, header.data(), header.size(), true)) {
            if (!saw_stream_identifier_) {
                throw snappy_stream_error("missing stream identifier");
            }
            at_end_ = true;
            return;
        }

        const auto type = static_cast<std::uint8_t>(header[0]);
        const std::size_t length = load_little_endian_24(header.data() + 1);

        if (!saw_stream_identifier_ && type != kStreamIdentifierChunk) {
            throw snappy_stream_error("missing stream identifier");
        }

        if (type == kStreamIdentifierChunk) {
            read_stream_identifier(source, length);
        } else if (type == kCompressedChunk || type == kUncompressedChunk) {
            read_data_chunk(source, type, length);
        } else if (type >= kFirstSkippableChunk) {
            skip_exact(source, length);
        } else {
            throw snappy_stream_error("unsupported unskippable chunk type");
        }
    }

    template <typename Source>
    void read_stream_identifier(Source& source, std::size_t length) {
        if (length != kStreamIdentifier.size()) {
            throw snappy_stream_error("invalid stream identifier length");
        }

        std::array<char, kStreamIdentifier.size()> identifier{};
        read_exact(source, identifier.data(), identifier.size(), false);
        if (identifier != kStreamIdentifier) {
            throw snappy_stream_error("invalid stream identifier");
        }
        saw_stream_identifier_ = true;
    }

    template <typename Source>
    void read_data_chunk(Source& source, std::uint8_t type, std::size_t length) {
        if (length < sizeof(std::uint32_t)) {
            throw snappy_stream_error("data chunk is missing its checksum");
        }

        const std::size_t payload_size = length - sizeof(std::uint32_t);
        const std::size_t maximum_payload = type == kCompressedChunk
                                                ? snappy::MaxCompressedLength(kMaxSnappyChunkSize)
                                                : kMaxSnappyChunkSize;
        if (payload_size > maximum_payload) {
            throw snappy_stream_error("data chunk is too large");
        }

        std::array<char, sizeof(std::uint32_t)> checksum_bytes{};
        read_exact(source, checksum_bytes.data(), checksum_bytes.size(), false);
        const std::uint32_t expected_checksum =
            load_little_endian_32(checksum_bytes.data());

        std::vector<char> payload(payload_size);
        if (!payload.empty()) {
            read_exact(source, payload.data(), payload.size(), false);
        }

        if (type == kCompressedChunk) {
            std::size_t uncompressed_size = 0;
            if (!snappy::GetUncompressedLength(
                    payload.data(), payload.size(), &uncompressed_size) ||
                uncompressed_size > kMaxSnappyChunkSize) {
                throw snappy_stream_error("invalid compressed chunk");
            }

            output_.resize(uncompressed_size);
            if (!snappy::RawUncompress(payload.data(), payload.size(), output_.data())) {
                throw snappy_stream_error("invalid compressed chunk");
            }
        } else {
            output_.assign(payload.begin(), payload.end());
        }

        const std::uint32_t actual_checksum =
            mask_crc32c(crc32c::Crc32c(output_.data(), output_.size()));
        if (actual_checksum != expected_checksum) {
            throw snappy_stream_error("checksum mismatch");
        }
        output_offset_ = 0;
    }

    std::string output_;
    std::size_t output_offset_ = 0;
    bool saw_stream_identifier_ = false;
    bool at_end_ = false;
};

void process_client(tcp::socket socket) {
    const std::string peer = peer_name(socket);
    auto shared_socket = std::make_shared<tcp::socket>(std::move(socket));

    try {
        boost::iostreams::filtering_istream decompressed;
        decompressed.push(snappy_framed_decompressor());
        decompressed.push(socket_source(std::move(shared_socket)));
        decompressed.exceptions(std::ios::badbit);

        std::string item;
        while (std::getline(decompressed, item, kDelimiter)) {
            print_item(item);
        }
    } catch (const snappy_stream_error& error) {
        report_error(peer, std::string("invalid snappy stream: ") + error.what());
    } catch (const boost::system::system_error& error) {
        report_error(peer, std::string("socket error: ") + error.what());
    } catch (const std::ios_base::failure& error) {
        report_error(peer, std::string("stream error: ") + error.what());
    } catch (const std::exception& error) {
        report_error(peer, std::string("unexpected error: ") + error.what());
    }
}

unsigned int worker_count() {
    return std::max(1U, std::thread::hardware_concurrency());
}

}  // namespace

int main() {
#ifdef _WIN32
    // Do not let the C runtime alter payload carriage returns on stdout.
    _setmode(_fileno(stdout), _O_BINARY);
#endif

    try {
        boost::asio::io_context io_context;
        const tcp::endpoint endpoint(tcp::v4(), kPort);
        tcp::acceptor acceptor(io_context, endpoint);
        boost::asio::thread_pool workers(worker_count());

        for (;;) {
            boost::system::error_code error;
            tcp::socket socket(io_context);
            acceptor.accept(socket, error);

            if (error) {
                const std::lock_guard<std::mutex> lock(error_mutex);
                std::cerr << "accept error: " << error.message() << '\n';
                continue;
            }

            boost::asio::post(
                workers,
                [client = std::move(socket)]() mutable {
                    process_client(std::move(client));
                });
        }
    } catch (const std::exception& error) {
        std::cerr << "server startup error: " << error.what() << '\n';
        return 1;
    }
}
