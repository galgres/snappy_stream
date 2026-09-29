#include <boost/asio.hpp>
#include <boost/iostreams/categories.hpp>
#include <boost/iostreams/operations.hpp>
#include <crc32c/crc32c.h>
#include <snappy.h>
#include <zlib.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <exception>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
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

enum class compression {
    gzip,
    snappy,
    none,
};

std::mutex output_mutex;
std::mutex error_mutex;

void report_server_error(const std::string& message) {
    const std::lock_guard<std::mutex> lock(error_mutex);
    std::cerr << message << '\n';
}

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

class chunk_source {
public:
    using char_type = char;
    using category = boost::iostreams::source_tag;

    void set_chunk(const char* data, std::size_t size) {
        if (offset_ != 0) {
            data_.erase(data_.begin(), data_.begin() + offset_);
            offset_ = 0;
        }
        data_.insert(data_.end(), data, data + size);
    }

    void finish() {
        finished_ = true;
    }

    std::streamsize read(char* destination, std::streamsize size) {
        if (size <= 0) {
            return 0;
        }

        const std::size_t available = data_.size() - offset_;
        if (available == 0) {
            return finished_ ? -1 : 0;
        }

        const std::size_t count = (std::min)(
            available,
            static_cast<std::size_t>(size));
        std::memcpy(destination, data_.data() + offset_, count);
        offset_ += count;
        return static_cast<std::streamsize>(count);
    }

private:
    std::vector<char> data_;
    std::size_t offset_ = 0;
    bool finished_ = false;
};

class snappy_stream_error : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

class gzip_stream_error : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

class gzip_decompressor {
public:
    gzip_decompressor() {
        const int result = inflateInit2(&stream_, MAX_WBITS + 16);
        if (result != Z_OK) {
            throw std::runtime_error(
                "could not initialize zlib gzip decompressor (error " +
                std::to_string(result) + ')');
        }
        initialized_ = true;
    }

    ~gzip_decompressor() {
        if (initialized_) {
            inflateEnd(&stream_);
        }
    }

    gzip_decompressor(const gzip_decompressor&) = delete;
    gzip_decompressor& operator=(const gzip_decompressor&) = delete;

    template <typename Consumer>
    void write(const char* data, std::size_t size, Consumer&& consume) {
        while (size != 0) {
            const std::size_t input_size = (std::min)(
                size,
                static_cast<std::size_t>((std::numeric_limits<uInt>::max)()));
            stream_.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(data));
            stream_.avail_in = static_cast<uInt>(input_size);
            at_member_boundary_ = false;

            for (;;) {
                stream_.next_out = output_.data();
                stream_.avail_out = static_cast<uInt>(output_.size());

                const uInt input_before = stream_.avail_in;
                const int result = inflate(&stream_, Z_NO_FLUSH);
                const std::size_t produced = output_.size() - stream_.avail_out;
                if (produced != 0) {
                    consume(
                        reinterpret_cast<const char*>(output_.data()),
                        produced);
                }

                if (result == Z_STREAM_END) {
                    saw_complete_member_ = true;
                    at_member_boundary_ = true;

                    Bytef* const remaining_input = stream_.next_in;
                    const uInt remaining_size = stream_.avail_in;
                    const int reset_result = inflateReset(&stream_);
                    if (reset_result != Z_OK) {
                        throw_zlib_error("could not reset gzip decompressor", reset_result);
                    }
                    stream_.next_in = remaining_input;
                    stream_.avail_in = remaining_size;

                    if (remaining_size == 0) {
                        break;
                    }
                    at_member_boundary_ = false;
                    continue;
                }

                if (result == Z_OK) {
                    if (stream_.avail_in == 0 && stream_.avail_out != 0) {
                        break;
                    }
                    if (stream_.avail_in == input_before && produced == 0) {
                        throw std::runtime_error(
                            "zlib gzip decompressor made no progress");
                    }
                    continue;
                }

                if (result == Z_BUF_ERROR && stream_.avail_in == 0) {
                    break;
                }
                throw_zlib_error("invalid gzip data", result);
            }

            data += input_size;
            size -= input_size;
        }
    }

    void finish() const {
        if (!saw_complete_member_) {
            throw gzip_stream_error("missing or incomplete gzip member");
        }
        if (!at_member_boundary_) {
            throw gzip_stream_error("truncated gzip member");
        }
    }

private:
    [[noreturn]] void throw_zlib_error(const char* context, int result) const {
        std::string message(context);
        if (stream_.msg != nullptr) {
            message += ": ";
            message += stream_.msg;
        } else {
            message += " (zlib error ";
            message += std::to_string(result);
            message += ')';
        }

        if (result == Z_DATA_ERROR || result == Z_NEED_DICT ||
            result == Z_BUF_ERROR) {
            throw gzip_stream_error(message);
        }
        throw std::runtime_error(message);
    }

    z_stream stream_{};
    std::array<Bytef, 8192> output_{};
    bool initialized_ = false;
    bool saw_complete_member_ = false;
    bool at_member_boundary_ = true;
};

class passthrough_filter {
public:
    using char_type = char;
    using category = boost::iostreams::multichar_input_filter_tag;

    template <typename Source>
    std::streamsize read(Source& source, char* destination, std::streamsize size) {
        if (size <= 0) {
            return 0;
        }
        return boost::iostreams::read(source, destination, size);
    }
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

        read_source(source);

        std::streamsize written = 0;
        while (written < size) {
            if (output_offset_ < output_.size()) {
                const std::size_t requested = static_cast<std::size_t>(size - written);
                const std::size_t available = output_.size() - output_offset_;
                const std::size_t count = (std::min)(requested, available);
                std::memcpy(destination + written, output_.data() + output_offset_, count);
                output_offset_ += count;
                written += static_cast<std::streamsize>(count);
                if (output_offset_ == output_.size()) {
                    break;
                }
                continue;
            }

            output_.clear();
            output_offset_ = 0;
            if (!read_next_chunk()) {
                break;
            }
        }

        if (written != 0) {
            return written;
        }
        return at_end_ ? -1 : 0;
    }

private:
    template <typename Source>
    void read_source(Source& source) {
        if (source_finished_) {
            return;
        }

        std::array<char, 8192> buffer{};
        for (;;) {
            const std::streamsize count = boost::iostreams::read(
                source, buffer.data(), static_cast<std::streamsize>(buffer.size()));
            if (count > 0) {
                input_.insert(input_.end(), buffer.data(), buffer.data() + count);
                continue;
            }
            if (count == -1) {
                source_finished_ = true;
            }
            return;
        }
    }

    bool read_next_chunk() {
        if (input_.size() < 4) {
            if (!source_finished_) {
                return false;
            }
            if (!input_.empty()) {
                throw snappy_stream_error("truncated chunk header");
            }
            if (!saw_stream_identifier_) {
                throw snappy_stream_error("missing stream identifier");
            }
            at_end_ = true;
            return false;
        }

        const auto type = static_cast<std::uint8_t>(input_[0]);
        const std::size_t length = load_little_endian_24(input_.data() + 1);
        if (!saw_stream_identifier_ && type != kStreamIdentifierChunk) {
            throw snappy_stream_error("missing stream identifier");
        }
        validate_chunk_header(type, length);

        if (input_.size() < 4 + length) {
            if (source_finished_) {
                throw snappy_stream_error("truncated chunk");
            }
            return false;
        }

        const char* const payload = input_.data() + 4;
        if (type == kStreamIdentifierChunk) {
            read_stream_identifier(payload, length);
        } else if (type == kCompressedChunk || type == kUncompressedChunk) {
            read_data_chunk(type, payload, length);
        } else if (type < kFirstSkippableChunk) {
            throw snappy_stream_error("unsupported unskippable chunk type");
        }

        input_.erase(input_.begin(), input_.begin() + 4 + length);
        return true;
    }

    void validate_chunk_header(std::uint8_t type, std::size_t length) const {
        if (type == kStreamIdentifierChunk) {
            if (length != kStreamIdentifier.size()) {
                throw snappy_stream_error("invalid stream identifier length");
            }
            return;
        }
        if (type == kCompressedChunk || type == kUncompressedChunk) {
            if (length < sizeof(std::uint32_t)) {
                throw snappy_stream_error("data chunk is missing its checksum");
            }
            const std::size_t data_size = length - sizeof(std::uint32_t);
            const std::size_t maximum_size =
                type == kCompressedChunk
                    ? snappy::MaxCompressedLength(kMaxSnappyChunkSize)
                    : kMaxSnappyChunkSize;
            if (data_size > maximum_size) {
                throw snappy_stream_error("data chunk is too large");
            }
            return;
        }
        if (type < kFirstSkippableChunk) {
            throw snappy_stream_error("unsupported unskippable chunk type");
        }
    }

    void read_stream_identifier(const char* payload, std::size_t length) {
        if (length != kStreamIdentifier.size() ||
            !std::equal(kStreamIdentifier.begin(), kStreamIdentifier.end(), payload)) {
            throw snappy_stream_error("invalid stream identifier");
        }
        saw_stream_identifier_ = true;
    }

    void read_data_chunk(std::uint8_t type, const char* payload, std::size_t length) {
        if (length < sizeof(std::uint32_t)) {
            throw snappy_stream_error("data chunk is missing its checksum");
        }

        const std::uint32_t expected_checksum = load_little_endian_32(payload);
        const char* const data = payload + sizeof(std::uint32_t);
        const std::size_t data_size = length - sizeof(std::uint32_t);

        if (type == kCompressedChunk) {
            if (data_size > snappy::MaxCompressedLength(kMaxSnappyChunkSize)) {
                throw snappy_stream_error("compressed data chunk is too large");
            }

            std::size_t uncompressed_size = 0;
            if (!snappy::GetUncompressedLength(data, data_size, &uncompressed_size) ||
                uncompressed_size > kMaxSnappyChunkSize) {
                throw snappy_stream_error("invalid compressed chunk");
            }

            output_.resize(uncompressed_size);
            if (!snappy::RawUncompress(data, data_size, output_.data())) {
                throw snappy_stream_error("invalid compressed chunk");
            }
        } else {
            if (data_size > kMaxSnappyChunkSize) {
                throw snappy_stream_error("uncompressed data chunk is too large");
            }
            output_.assign(data, data + data_size);
        }

        const std::uint32_t actual_checksum =
            mask_crc32c(crc32c::Crc32c(output_.data(), output_.size()));
        if (actual_checksum != expected_checksum) {
            throw snappy_stream_error("checksum mismatch");
        }
    }

    std::vector<char> input_;
    std::string output_;
    std::size_t output_offset_ = 0;
    bool saw_stream_identifier_ = false;
    bool source_finished_ = false;
    bool at_end_ = false;
};

template <typename Decompressor>
class client_session
    : public std::enable_shared_from_this<client_session<Decompressor>> {
public:
    explicit client_session(tcp::socket socket)
        : socket_(std::move(socket)),
          peer_(peer_name(socket_)) {}

    void start() {
        read_next_chunk();
    }

private:
    void read_next_chunk() {
        auto self = this->shared_from_this();
        socket_.async_read_some(
            boost::asio::buffer(input_buffer_),
            [self](const boost::system::error_code& error, std::size_t count) {
                self->handle_read(error, count);
            });
    }

    void handle_read(const boost::system::error_code& error, std::size_t count) {
        try {
            if (count != 0) {
                process_input(input_buffer_.data(), count);
            }

            if (!error) {
                read_next_chunk();
                return;
            }

            if (error == boost::asio::error::eof) {
                finish_decompressor();
                finish_current_item();
                return;
            }

            report_error(peer_, std::string("socket error: ") + error.message());
        } catch (const gzip_stream_error& exception) {
            report_error(peer_, std::string("invalid gzip stream: ") + exception.what());
        } catch (const snappy_stream_error& exception) {
            report_error(peer_, std::string("invalid snappy stream: ") + exception.what());
        } catch (const boost::system::system_error& exception) {
            report_error(peer_, std::string("socket error: ") + exception.what());
        } catch (const std::exception& exception) {
            report_error(peer_, std::string("unexpected error: ") + exception.what());
        }
    }

    void process_input(const char* data, std::size_t size) {
        if constexpr (std::is_same_v<
                          Decompressor,
                          gzip_decompressor>) {
            decompressor_.write(
                data,
                size,
                [this](const char* output, std::size_t output_size) {
                    consume_decompressed(output, output_size);
                });
        } else {
            compressed_.set_chunk(data, size);
            drain_decompressor();
        }
    }

    void finish_decompressor() {
        if constexpr (std::is_same_v<
                          Decompressor,
                          gzip_decompressor>) {
            decompressor_.finish();
        } else {
            compressed_.finish();
            drain_decompressor();
        }
    }

    void drain_decompressor() {
        for (;;) {
            const std::streamsize count = decompressor_.read(
                compressed_,
                output_buffer_.data(),
                static_cast<std::streamsize>(output_buffer_.size()));

            if (count == -1) {
                finish_current_item();
                return;
            }
            if (count == 0) {
                return;
            }

            consume_decompressed(
                output_buffer_.data(),
                static_cast<std::size_t>(count));
        }
    }

    void consume_decompressed(const char* data, std::size_t size) {
        const char* current = data;
        const char* const end = data + size;

        while (current != end) {
            const char* const delimiter = std::find(current, end, kDelimiter);
            current_item_.append(
                current,
                static_cast<std::size_t>(delimiter - current));

            if (delimiter == end) {
                return;
            }

            print_item(current_item_);
            current_item_.clear();
            current = delimiter + 1;
        }
    }

    void finish_current_item() {
        if (!current_item_.empty()) {
            print_item(current_item_);
            current_item_.clear();
        }
    }

    tcp::socket socket_;
    std::string peer_;
    std::array<char, 8192> input_buffer_{};
    std::array<char, 8192> output_buffer_{};
    chunk_source compressed_;
    Decompressor decompressor_;
    std::string current_item_;
};

template <typename Decompressor>
class server {
public:
    server(boost::asio::io_context& io_context, const tcp::endpoint& endpoint)
        : acceptor_(io_context, endpoint) {}

    void start() {
        accept_next_client();
    }

private:
    void accept_next_client() {
        acceptor_.async_accept(
            [this](const boost::system::error_code& error, tcp::socket socket) {
                try {
                    if (error) {
                        report_server_error(std::string("accept error: ") + error.message());
                    } else {
                        std::make_shared<client_session<Decompressor>>(
                            std::move(socket))->start();
                    }

                    accept_next_client();
                } catch (const std::exception& exception) {
                    report_server_error(
                        std::string("accept handler error: ") + exception.what());
                }
            });
    }

    tcp::acceptor acceptor_;
};

unsigned int worker_count() {
    return std::max(1U, std::thread::hardware_concurrency());
}

compression parse_arguments(int argc, char* argv[]) {
    compression selected = compression::gzip;
    bool compression_was_set = false;

    for (int index = 1; index < argc; ++index) {
        std::string argument = argv[index];
        std::string value;
        if (argument == "--compression") {
            if (++index == argc) {
                throw std::invalid_argument(
                    "--compression requires gzip, snappy, or none");
            }
            value = argv[index];
        } else if (argument.rfind("--compression=", 0) == 0) {
            value = argument.substr(std::string("--compression=").size());
        } else {
            throw std::invalid_argument("unknown argument: " + argument);
        }

        if (compression_was_set) {
            throw std::invalid_argument("--compression may only be specified once");
        }
        compression_was_set = true;

        if (value == "gzip") {
            selected = compression::gzip;
        } else if (value == "snappy") {
            selected = compression::snappy;
        } else if (value == "none") {
            selected = compression::none;
        } else {
            throw std::invalid_argument("unsupported compression: " + value);
        }
    }

    return selected;
}

template <typename Decompressor>
void run_server() {
    boost::asio::io_context io_context;
    const tcp::endpoint endpoint(tcp::v4(), kPort);
    server<Decompressor> listener(io_context, endpoint);
    listener.start();

    const unsigned int thread_count = worker_count();
    boost::asio::thread_pool workers(thread_count);
    for (unsigned int index = 0; index < thread_count; ++index) {
        boost::asio::post(
            workers,
            [&io_context] {
                try {
                    io_context.run();
                } catch (const std::exception& exception) {
                    report_server_error(
                        std::string("I/O worker error: ") + exception.what());
                }
            });
    }
    workers.join();
}

}  // namespace

int main(int argc, char* argv[]) {
#ifdef _WIN32
    // Do not let the C runtime alter payload carriage returns on stdout.
    _setmode(_fileno(stdout), _O_BINARY);
#endif

    try {
        switch (parse_arguments(argc, argv)) {
        case compression::gzip:
            run_server<gzip_decompressor>();
            break;
        case compression::snappy:
            run_server<snappy_framed_decompressor>();
            break;
        case compression::none:
            run_server<passthrough_filter>();
            break;
        }
    } catch (const std::invalid_argument& error) {
        std::cerr << "argument error: " << error.what() << '\n'
                  << "usage: " << argv[0]
                  << " [--compression gzip|snappy|none]\n";
        return 2;
    } catch (const std::exception& error) {
        std::cerr << "server startup error: " << error.what() << '\n';
        return 1;
    }
}
