#include <boost/asio.hpp>
#include <boost/iostreams/categories.hpp>
#include <boost/iostreams/filter/gzip.hpp>

#include <algorithm>
#include <array>
#include <cstring>
#include <exception>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <utility>

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#endif

namespace {

using boost::asio::ip::tcp;

constexpr unsigned short kPort = 8080;
constexpr char kDelimiter = ';';

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
        data_ = data;
        size_ = size;
        offset_ = 0;
    }

    void finish() {
        finished_ = true;
    }

    std::streamsize read(char* destination, std::streamsize size) {
        if (size <= 0) {
            return 0;
        }

        const std::size_t available = size_ - offset_;
        if (available == 0) {
            return finished_ ? -1 : 0;
        }

        const std::size_t count = (std::min)(
            available,
            static_cast<std::size_t>(size));
        std::memcpy(destination, data_ + offset_, count);
        offset_ += count;
        return static_cast<std::streamsize>(count);
    }

private:
    const char* data_ = nullptr;
    std::size_t size_ = 0;
    std::size_t offset_ = 0;
    bool finished_ = false;
};

class client_session : public std::enable_shared_from_this<client_session> {
public:
    explicit client_session(tcp::socket socket)
        : socket_(std::move(socket)), peer_(peer_name(socket_)) {}

    void start() {
        read_next_chunk();
    }

private:
    void read_next_chunk() {
        auto self = shared_from_this();
        socket_.async_read_some(
            boost::asio::buffer(input_buffer_),
            [self](const boost::system::error_code& error, std::size_t count) {
                self->handle_read(error, count);
            });
    }

    void handle_read(const boost::system::error_code& error, std::size_t count) {
        try {
            if (count != 0) {
                compressed_.set_chunk(input_buffer_.data(), count);
                drain_decompressor();
            }

            if (!error) {
                read_next_chunk();
                return;
            }

            if (error == boost::asio::error::eof) {
                compressed_.finish();
                drain_decompressor();
                return;
            }

            report_error(peer_, std::string("socket error: ") + error.message());
        } catch (const boost::iostreams::gzip_error& exception) {
            report_error(peer_, std::string("invalid gzip stream: ") + exception.what());
        } catch (const boost::system::system_error& exception) {
            report_error(peer_, std::string("socket error: ") + exception.what());
        } catch (const std::exception& exception) {
            report_error(peer_, std::string("unexpected error: ") + exception.what());
        }
    }

    void drain_decompressor() {
        for (;;) {
            const std::streamsize count = decompressor_.read(
                compressed_,
                output_buffer_.data(),
                static_cast<std::streamsize>(output_buffer_.size()));

            if (count == -1) {
                if (!current_item_.empty()) {
                    print_item(current_item_);
                    current_item_.clear();
                }
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

    tcp::socket socket_;
    std::string peer_;
    std::array<char, 8192> input_buffer_{};
    std::array<char, 8192> output_buffer_{};
    chunk_source compressed_;
    boost::iostreams::gzip_decompressor decompressor_;
    std::string current_item_;
};

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
                        std::make_shared<client_session>(std::move(socket))->start();
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

}  // namespace

int main() {
#ifdef _WIN32
    // Do not let the C runtime alter payload carriage returns on stdout.
    _setmode(_fileno(stdout), _O_BINARY);
#endif

    try {
        boost::asio::io_context io_context;
        const tcp::endpoint endpoint(tcp::v4(), kPort);
        server listener(io_context, endpoint);
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
    } catch (const std::exception& error) {
        std::cerr << "server startup error: " << error.what() << '\n';
        return 1;
    }
}
