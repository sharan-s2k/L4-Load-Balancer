#pragma once
#include "event_loop.h"
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace lb {

enum class Algorithm { RoundRobin, LeastConnections, ConsistentHash };
enum class BackendMode { Active, Draining, Disabled };
enum class CircuitState { Closed, Open, HalfOpen };

struct BackendConfig {
    std::string name;
    std::string host;
    std::string port;
    std::uint64_t max_connections{10000};
};

struct BackendSnapshot {
    std::size_t index{};
    BackendConfig config;
    bool healthy{true};
    BackendMode mode{BackendMode::Active};
    CircuitState circuit{CircuitState::Closed};
    std::uint64_t active{0};
    std::uint64_t total{0};
    std::uint64_t failures{0};
};

class BackendPool {
public:
    BackendPool(std::vector<BackendConfig>, Algorithm,
                std::uint32_t failure_threshold,
                std::chrono::milliseconds circuit_open_time);
    ~BackendPool();

    std::optional<std::size_t> choose(const std::string& affinity_key);
    BackendConfig config(std::size_t) const;
    void opened(std::size_t);
    void closed(std::size_t);
    void connectSucceeded(std::size_t);
    void connectFailed(std::size_t);

    void startHealthChecks(int interval_ms, int timeout_ms);
    void stopHealthChecks();

    bool drain(std::size_t);
    bool activate(std::size_t);
    bool disable(std::size_t);
    std::vector<BackendSnapshot> snapshot() const;

private:
    struct State {
        BackendConfig cfg;
        bool healthy{true};
        BackendMode mode{BackendMode::Active};
        CircuitState circuit{CircuitState::Closed};
        std::uint64_t active{0};
        std::uint64_t total{0};
        std::uint64_t failures{0};
        std::uint32_t consecutive_failures{0};
        std::chrono::steady_clock::time_point opened_at{};
        bool half_open_probe_in_flight{false};
    };

    bool eligible(State&, std::chrono::steady_clock::time_point);
    bool probe(const BackendConfig&, int timeout_ms) const;
    void healthLoop(int interval_ms, int timeout_ms);

    mutable std::mutex mu_;
    std::vector<State> states_;
    Algorithm algorithm_;
    std::size_t cursor_{0};
    std::uint32_t failure_threshold_;
    std::chrono::milliseconds circuit_open_time_;
    std::atomic<bool> checking_{false};
    std::thread checker_;
};

struct Metrics {
    std::atomic<std::uint64_t> accepted{0};
    std::atomic<std::uint64_t> active{0};
    std::atomic<std::uint64_t> closed{0};
    std::atomic<std::uint64_t> rejected{0};
    std::atomic<std::uint64_t> connect_failures{0};
    std::atomic<std::uint64_t> bytes_c2b{0};
    std::atomic<std::uint64_t> bytes_b2c{0};
    std::atomic<std::uint64_t> backpressure_pauses{0};
    std::atomic<std::uint64_t> idle_timeouts{0};
};

struct Config {
    std::string listen_host{"127.0.0.1"};
    std::string listen_port{"8080"};
    std::size_t high_watermark{1024 * 1024};
    std::size_t low_watermark{512 * 1024};
    std::uint64_t global_connection_limit{50000};
    std::chrono::milliseconds connect_timeout{3000};
    std::chrono::milliseconds idle_timeout{60000};
};

class LoadBalancer {
public:
    LoadBalancer(Config, BackendPool&, Metrics&);
    ~LoadBalancer();
    void run();
    void stop();

private:
    struct Buffer {
        std::vector<char> data;
        std::size_t offset{0};
        std::size_t size() const { return data.size() - offset; }
        bool empty() const { return size() == 0; }
        const char* begin() const { return data.data() + offset; }
        void append(const char*, std::size_t);
        void consume(std::size_t);
    };
    struct Session {
        int client{-1};
        int backend{-1};
        std::size_t backend_index{};
        bool connecting{true};
        bool pause_client{false};
        bool pause_backend{false};
        bool client_read_closed{false};
        bool backend_read_closed{false};
        bool backend_write_shutdown{false};
        bool client_write_shutdown{false};
        bool closed{false};
        Buffer c2b, b2c;
        std::chrono::steady_clock::time_point created, last;
    };
    enum class Side { Client, Backend };
    enum class ReadResult { Open, EndOfStream, Error };
    struct Ref { std::shared_ptr<Session> session; Side side; };

    int createListener();
    void acceptAll();
    void openSession(int client_fd, const std::string& affinity_key);
    int connectBackend(std::size_t, bool&);
    void handle(const ReadyEvent&);
    void readable(const std::shared_ptr<Session>&, Side);
    void writable(const std::shared_ptr<Session>&, Side);
    void finishConnect(const std::shared_ptr<Session>&);
    ReadResult readInto(int, Buffer&, std::atomic<std::uint64_t>&);
    bool flush(int, Buffer&);
    void propagateHalfCloses(const std::shared_ptr<Session>&);
    bool finished(const std::shared_ptr<Session>&) const;
    void update(const std::shared_ptr<Session>&);
    void closeSession(const std::shared_ptr<Session>&);
    void sweep();

    Config cfg_;
    BackendPool& pool_;
    Metrics& metrics_;
    EventLoop loop_;
    int listener_{-1};
    std::atomic<bool> running_{false};
    std::unordered_map<int, Ref> refs_;
    std::vector<std::shared_ptr<Session>> sessions_;
    std::chrono::steady_clock::time_point next_sweep_;
};

class AdminServer {
public:
    AdminServer(std::string host, std::string port,
                BackendPool&, const Metrics&);
    ~AdminServer();
    void start();
    void stop();

private:
    void run();
    std::string metricsText() const;
    std::string handleRequest(const std::string& request);

    std::string host_, port_;
    BackendPool& pool_;
    const Metrics& metrics_;
    std::atomic<bool> running_{false};
    std::thread thread_;
    int listener_{-1};
};

} // namespace lb
