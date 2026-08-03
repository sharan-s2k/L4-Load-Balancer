#include "common.h"
#include "load_balancer.h"
#include <csignal>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

static lb::LoadBalancer* g_lb = nullptr;
static void signalHandler(int) { if (g_lb) g_lb->stop(); }

static void usage() {
    std::cout <<
      "tcp-load-balancer --listen HOST:PORT --backend NAME=HOST:PORT [--backend ...]\n"
      "  [--algorithm round-robin|least-connections|consistent-hash]\n"
      "  [--admin-listen HOST:PORT] [--global-connection-limit N]\n"
      "  [--backend-max-connections N] [--health-interval-ms N]\n"
      "  [--health-timeout-ms N] [--circuit-failure-threshold N]\n"
      "  [--circuit-open-ms N] [--connect-timeout-ms N] [--idle-timeout-ms N]\n"
      "  [--high-watermark-bytes N] [--low-watermark-bytes N]\n";
}

int main(int argc, char** argv) {
    try {
        lb::Config cfg;
        std::vector<lb::BackendConfig> backends;
        lb::Algorithm algo = lb::Algorithm::RoundRobin;
        std::string admin_host = "127.0.0.1", admin_port = "9090";
        int health_interval = 2000, health_timeout = 500, circuit_open_ms = 5000;
        std::uint32_t failure_threshold = 3;
        std::uint64_t backend_max = 10000;

        for (int i = 1; i < argc; ++i) {
            std::string a = argv[i];
            auto value = [&]() {
                if (i + 1 >= argc) throw std::runtime_error("missing value for " + a);
                return std::string(argv[++i]);
            };
            if (a == "--listen") {
                auto [h, p] = lb::splitHostPort(value()); cfg.listen_host = h; cfg.listen_port = p;
            } else if (a == "--backend") {
                auto raw = value(); auto eq = raw.find('=');
                if (eq == std::string::npos) throw std::runtime_error("backend must be NAME=HOST:PORT");
                auto [h, p] = lb::splitHostPort(raw.substr(eq + 1));
                backends.push_back({raw.substr(0, eq), h, p, backend_max});
            } else if (a == "--algorithm") {
                auto v = value();
                if (v == "round-robin") algo = lb::Algorithm::RoundRobin;
                else if (v == "least-connections") algo = lb::Algorithm::LeastConnections;
                else if (v == "consistent-hash") algo = lb::Algorithm::ConsistentHash;
                else throw std::runtime_error("unknown algorithm: " + v);
            } else if (a == "--admin-listen") {
                auto [h, p] = lb::splitHostPort(value()); admin_host = h; admin_port = p;
            } else if (a == "--global-connection-limit") cfg.global_connection_limit = std::stoull(value());
            else if (a == "--backend-max-connections") backend_max = std::stoull(value());
            else if (a == "--health-interval-ms") health_interval = std::stoi(value());
            else if (a == "--health-timeout-ms") health_timeout = std::stoi(value());
            else if (a == "--circuit-failure-threshold") failure_threshold = static_cast<std::uint32_t>(std::stoul(value()));
            else if (a == "--circuit-open-ms") circuit_open_ms = std::stoi(value());
            else if (a == "--connect-timeout-ms") cfg.connect_timeout = std::chrono::milliseconds(std::stoi(value()));
            else if (a == "--idle-timeout-ms") cfg.idle_timeout = std::chrono::milliseconds(std::stoi(value()));
            else if (a == "--high-watermark-bytes") cfg.high_watermark = std::stoull(value());
            else if (a == "--low-watermark-bytes") cfg.low_watermark = std::stoull(value());
            else if (a == "--help" || a == "-h") { usage(); return 0; }
            else throw std::runtime_error("unknown argument: " + a);
        }

        if (backends.empty()) { usage(); throw std::runtime_error("at least one backend is required"); }
        for (auto& b : backends) b.max_connections = backend_max;

        lb::BackendPool pool(std::move(backends), algo, failure_threshold,
                             std::chrono::milliseconds(circuit_open_ms));
        lb::Metrics metrics;
        lb::AdminServer admin(admin_host, admin_port, pool, metrics);
        lb::LoadBalancer balancer(cfg, pool, metrics);

        pool.startHealthChecks(health_interval, health_timeout);
        admin.start();
        g_lb = &balancer;
        std::signal(SIGINT, signalHandler);
        std::signal(SIGTERM, signalHandler);
        balancer.run();
        admin.stop();
        pool.stopHealthChecks();
        g_lb = nullptr;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "tcp-load-balancer error: " << e.what() << '\n';
        return 1;
    }
}
