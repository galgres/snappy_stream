#include <boost/asio.hpp>
#include <boost/iostreams/categories.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filtering_stream.hpp>

#include <algorithm>
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

void process_client(tcp::socket socket) {
    const std::string peer = peer_name(socket);
    auto shared_socket = std::make_shared<tcp::socket>(std::move(socket));

    try {
        boost::iostreams::filtering_istream decompressed;
        decompressed.push(boost::iostreams::gzip_decompressor());
        decompressed.push(socket_source(std::move(shared_socket)));
        decompressed.exceptions(std::ios::badbit);

        std::string item;
        while (std::getline(decompressed, item, kDelimiter)) {
            print_item(item);
        }
    } catch (const boost::iostreams::gzip_error& error) {
        report_error(peer, std::string("invalid gzip stream: ") + error.what());
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
