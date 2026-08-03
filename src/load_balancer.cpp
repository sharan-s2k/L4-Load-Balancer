#include "load_balancer.h"
#include "common.h"
#include <poll.h>
#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <functional>
#include <iostream>
#include <limits>
#include <sstream>
#include <stdexcept>

namespace lb {

static std::runtime_error syserr(const char* m) {
    return std::runtime_error(std::string(m) + ": " + std::strerror(errno));
}

static const char* circuitName(CircuitState s) {
    switch (s) {
        case CircuitState::Closed: return "closed";
        case CircuitState::Open: return "open";
        case CircuitState::HalfOpen: return "half_open";
    }
    return "unknown";
}

static const char* modeName(BackendMode s) {
    switch (s) {
        case BackendMode::Active: return "active";
        case BackendMode::Draining: return "draining";
        case BackendMode::Disabled: return "disabled";
    }
    return "unknown";
}

BackendPool::BackendPool(std::vector<BackendConfig> b, Algorithm a,
                         std::uint32_t threshold,
                         std::chrono::milliseconds open_time)
    : algorithm_(a), failure_threshold_(threshold),
      circuit_open_time_(open_time) {
    if (b.empty()) throw std::runtime_error("at least one backend required");
    for (auto& x : b) states_.push_back({std::move(x)});
}

BackendPool::~BackendPool() { stopHealthChecks(); }

bool BackendPool::eligible(State& s, std::chrono::steady_clock::time_point now) {
    if (!s.healthy || s.mode != BackendMode::Active ||
        s.active >= s.cfg.max_connections) return false;
    if (s.circuit == CircuitState::Closed) return true;
    if (s.circuit == CircuitState::Open) {
        if (now - s.opened_at < circuit_open_time_) return false;
        s.circuit = CircuitState::HalfOpen;
        s.half_open_probe_in_flight = false;
    }
    if (!s.half_open_probe_in_flight) {
        s.half_open_probe_in_flight = true;
        return true;
    }
    return false;
}

std::optional<std::size_t> BackendPool::choose(const std::string& key) {
    std::lock_guard l(mu_);
    const auto now = std::chrono::steady_clock::now();
    if (algorithm_ == Algorithm::RoundRobin) {
        for (std::size_t n = 0; n < states_.size(); ++n) {
            const auto i = (cursor_ + n) % states_.size();
            if (eligible(states_[i], now)) {
                cursor_ = (i + 1) % states_.size();
                return i;
            }
        }
        return std::nullopt;
    }
    if (algorithm_ == Algorithm::LeastConnections) {
        std::optional<std::size_t> best;
        std::uint64_t least = std::numeric_limits<std::uint64_t>::max();
        for (std::size_t i = 0; i < states_.size(); ++i) {
            if (eligible(states_[i], now) && states_[i].active < least) {
                best = i;
                least = states_[i].active;
            }
        }
        return best;
    }
    const std::size_t start = std::hash<std::string>{}(key) % states_.size();
    for (std::size_t n = 0; n < states_.size(); ++n) {
        const auto i = (start + n) % states_.size();
        if (eligible(states_[i], now)) return i;
    }
    return std::nullopt;
}

BackendConfig BackendPool::config(std::size_t i) const {
    std::lock_guard l(mu_);
    return states_.at(i).cfg;
}

void BackendPool::opened(std::size_t i) {
    std::lock_guard l(mu_);
    ++states_.at(i).active;
    ++states_.at(i).total;
}

void BackendPool::closed(std::size_t i) {
    std::lock_guard l(mu_);
    auto& s = states_.at(i);
    if (s.active) --s.active;
    if (s.mode == BackendMode::Draining && s.active == 0)
        s.mode = BackendMode::Disabled;
}

void BackendPool::connectSucceeded(std::size_t i) {
    std::lock_guard l(mu_);
    auto& s = states_.at(i);
    s.healthy = true;
    s.circuit = CircuitState::Closed;
    s.consecutive_failures = 0;
    s.half_open_probe_in_flight = false;
}

void BackendPool::connectFailed(std::size_t i) {
    std::lock_guard l(mu_);
    auto& s = states_.at(i);
    ++s.failures;
    ++s.consecutive_failures;
    s.half_open_probe_in_flight = false;
    if (s.circuit == CircuitState::HalfOpen ||
        s.consecutive_failures >= failure_threshold_) {
        s.circuit = CircuitState::Open;
        s.opened_at = std::chrono::steady_clock::now();
    }
}

void BackendPool::startHealthChecks(int interval, int timeout) {
    if (checking_.exchange(true)) return;
    checker_ = std::thread(&BackendPool::healthLoop, this, interval, timeout);
}

void BackendPool::stopHealthChecks() {
    if (!checking_.exchange(false)) return;
    if (checker_.joinable()) checker_.join();
}

bool BackendPool::probe(const BackendConfig& b, int timeout) const {
    try {
        auto a = resolve(b.host, b.port);
        int fd = ::socket(a.addr.ss_family, SOCK_STREAM, 0);
        if (fd < 0) return false;
        setNonBlocking(fd);
        int rc = ::connect(fd, reinterpret_cast<sockaddr*>(&a.addr), a.len);
        if (rc == 0) { closeFd(fd); return true; }
        if (errno != EINPROGRESS) { closeFd(fd); return false; }
        pollfd p{fd, POLLOUT, 0};
        rc = ::poll(&p, 1, timeout);
        int e = 0; socklen_t n = sizeof(e);
        bool ok = rc > 0 && ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &e, &n) == 0 && e == 0;
        closeFd(fd);
        return ok;
    } catch (...) { return false; }
}

void BackendPool::healthLoop(int interval, int timeout) {
    while (checking_) {
        std::vector<BackendConfig> cfgs;
        { std::lock_guard l(mu_); for (auto& s : states_) cfgs.push_back(s.cfg); }
        for (std::size_t i = 0; i < cfgs.size() && checking_; ++i) {
            const bool healthy = probe(cfgs[i], timeout);
            std::lock_guard l(mu_);
            states_[i].healthy = healthy;
            if (healthy && states_[i].circuit == CircuitState::HalfOpen) {
                states_[i].circuit = CircuitState::Closed;
                states_[i].consecutive_failures = 0;
                states_[i].half_open_probe_in_flight = false;
            }
        }
        for (int waited = 0; checking_ && waited < interval; waited += 50)
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
}

bool BackendPool::drain(std::size_t i) {
    std::lock_guard l(mu_);
    if (i >= states_.size()) return false;
    states_[i].mode = states_[i].active == 0 ? BackendMode::Disabled : BackendMode::Draining;
    return true;
}

bool BackendPool::activate(std::size_t i) {
    std::lock_guard l(mu_);
    if (i >= states_.size()) return false;
    states_[i].mode = BackendMode::Active;
    return true;
}

bool BackendPool::disable(std::size_t i) {
    std::lock_guard l(mu_);
    if (i >= states_.size()) return false;
    states_[i].mode = BackendMode::Disabled;
    return true;
}

std::vector<BackendSnapshot> BackendPool::snapshot() const {
    std::lock_guard l(mu_);
    std::vector<BackendSnapshot> out;
    for (std::size_t i = 0; i < states_.size(); ++i) {
        const auto& s = states_[i];
        out.push_back({i, s.cfg, s.healthy, s.mode, s.circuit,
                       s.active, s.total, s.failures});
    }
    return out;
}

void LoadBalancer::Buffer::append(const char* p, std::size_t n) { data.insert(data.end(), p, p + n); }
void LoadBalancer::Buffer::consume(std::size_t n) {
    offset += n;
    if (offset == data.size()) { data.clear(); offset = 0; }
    else if (offset > 65536 && offset * 2 > data.size()) {
        data.erase(data.begin(), data.begin() + static_cast<std::ptrdiff_t>(offset));
        offset = 0;
    }
}

LoadBalancer::LoadBalancer(Config c, BackendPool& p, Metrics& m)
    : cfg_(std::move(c)), pool_(p), metrics_(m) {
    if (cfg_.low_watermark >= cfg_.high_watermark)
        throw std::runtime_error("low watermark must be below high watermark");
}

LoadBalancer::~LoadBalancer() {
    stop();
    for (auto& s : sessions_) closeSession(s);
    closeFd(listener_);
}

int LoadBalancer::createListener() {
    auto a = resolve(cfg_.listen_host, cfg_.listen_port);
    int fd = ::socket(a.addr.ss_family, SOCK_STREAM, 0);
    if (fd < 0) throw syserr("socket");
    int one = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    if (::bind(fd, reinterpret_cast<sockaddr*>(&a.addr), a.len) < 0 ||
        ::listen(fd, SOMAXCONN) < 0) {
        closeFd(fd); throw syserr("bind/listen");
    }
    setNonBlocking(fd);
    return fd;
}

void LoadBalancer::run() {
    listener_ = createListener();
    loop_.add(listener_, EV_READ);
    running_ = true;
    next_sweep_ = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    std::cout << "listening on " << cfg_.listen_host << ':' << cfg_.listen_port << '\n';
    while (running_) {
        for (auto& e : loop_.wait(250)) {
            if (e.fd == listener_) { if (e.mask & EV_READ) acceptAll(); }
            else handle(e);
        }
        auto now = std::chrono::steady_clock::now();
        if (now >= next_sweep_) { sweep(); next_sweep_ = now + std::chrono::seconds(1); }
    }
}

void LoadBalancer::stop() { running_ = false; }

void LoadBalancer::acceptAll() {
    for (;;) {
        sockaddr_storage peer{}; socklen_t len = sizeof(peer);
        int c = ::accept(listener_, reinterpret_cast<sockaddr*>(&peer), &len);
        if (c < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) return;
            if (errno == EINTR) continue;
            throw syserr("accept");
        }
        if (metrics_.active >= cfg_.global_connection_limit) {
            ++metrics_.rejected; closeFd(c); continue;
        }
        try {
            setNonBlocking(c);
            char ip[INET6_ADDRSTRLEN]{};
            if (peer.ss_family == AF_INET)
                ::inet_ntop(AF_INET, &reinterpret_cast<sockaddr_in*>(&peer)->sin_addr, ip, sizeof(ip));
            else if (peer.ss_family == AF_INET6)
                ::inet_ntop(AF_INET6, &reinterpret_cast<sockaddr_in6*>(&peer)->sin6_addr, ip, sizeof(ip));
            openSession(c, ip);
        } catch (const std::exception& e) {
            std::cerr << "session error: " << e.what() << '\n'; closeFd(c);
        }
    }
}

void LoadBalancer::openSession(int c, const std::string& key) {
    auto bi = pool_.choose(key);
    if (!bi) { ++metrics_.rejected; closeFd(c); return; }
    bool connecting = true;
    int b = -1;
    try { b = connectBackend(*bi, connecting); }
    catch (...) { pool_.connectFailed(*bi); ++metrics_.connect_failures; closeFd(c); throw; }
    auto s = std::make_shared<Session>();
    s->client = c; s->backend = b; s->backend_index = *bi; s->connecting = connecting;
    s->created = s->last = std::chrono::steady_clock::now();
    pool_.opened(*bi);
    ++metrics_.accepted; ++metrics_.active;
    refs_[c] = {s, Side::Client}; refs_[b] = {s, Side::Backend}; sessions_.push_back(s);
    loop_.add(c, EV_READ); loop_.add(b, connecting ? EV_WRITE : EV_READ);
    if (!connecting) pool_.connectSucceeded(*bi);
}

int LoadBalancer::connectBackend(std::size_t i, bool& connecting) {
    auto c = pool_.config(i); auto a = resolve(c.host, c.port);
    int fd = ::socket(a.addr.ss_family, SOCK_STREAM, 0);
    if (fd < 0) throw syserr("backend socket");
    setNonBlocking(fd);
    int rc = ::connect(fd, reinterpret_cast<sockaddr*>(&a.addr), a.len);
    if (rc == 0) { connecting = false; return fd; }
    if (errno != EINPROGRESS) { closeFd(fd); throw syserr("backend connect"); }
    connecting = true; return fd;
}

void LoadBalancer::handle(const ReadyEvent& e) {
    auto it = refs_.find(e.fd);
    if (it == refs_.end()) return;

    auto s = it->second.session;
    const auto side = it->second.side;
    if (s->closed) return;

    if (side == Side::Backend && s->connecting &&
        (e.mask & (EV_WRITE | EV_ERROR))) {
        finishConnect(s);
        if (s->closed) return;
    }

    if (e.mask & EV_ERROR) {
        closeSession(s);
        return;
    }
    if (e.mask & EV_READ) readable(s, side);
    if (!s->closed && (e.mask & EV_WRITE)) writable(s, side);
}

void LoadBalancer::readable(const std::shared_ptr<Session>& s, Side side) {
    ReadResult result = ReadResult::Open;

    if (side == Side::Client) {
        if (s->client_read_closed) return;

        result = readInto(s->client, s->c2b, metrics_.bytes_c2b);
        if (result == ReadResult::EndOfStream) {
            s->client_read_closed = true;
        }
        if (s->c2b.size() >= cfg_.high_watermark && !s->pause_client) {
            s->pause_client = true;
            ++metrics_.backpressure_pauses;
        }
    } else if (!s->connecting) {
        if (s->backend_read_closed) return;

        result = readInto(s->backend, s->b2c, metrics_.bytes_b2c);
        if (result == ReadResult::EndOfStream) {
            s->backend_read_closed = true;
        }
        if (s->b2c.size() >= cfg_.high_watermark && !s->pause_backend) {
            s->pause_backend = true;
            ++metrics_.backpressure_pauses;
        }
    }

    if (result == ReadResult::Error) {
        closeSession(s);
        return;
    }

    s->last = std::chrono::steady_clock::now();
    propagateHalfCloses(s);
    if (s->closed) return;
    if (finished(s)) {
        closeSession(s);
        return;
    }
    update(s);
}

void LoadBalancer::writable(const std::shared_ptr<Session>& s, Side side) {
    bool ok = true;

    if (side == Side::Client) {
        ok = flush(s->client, s->b2c);
        if (s->b2c.size() <= cfg_.low_watermark) {
            s->pause_backend = false;
        }
    } else if (!s->connecting) {
        ok = flush(s->backend, s->c2b);
        if (s->c2b.size() <= cfg_.low_watermark) {
            s->pause_client = false;
        }
    }

    if (!ok) {
        closeSession(s);
        return;
    }

    s->last = std::chrono::steady_clock::now();
    propagateHalfCloses(s);
    if (s->closed) return;
    if (finished(s)) {
        closeSession(s);
        return;
    }
    update(s);
}

void LoadBalancer::finishConnect(const std::shared_ptr<Session>& s) {
    int error_code = 0;
    socklen_t length = sizeof(error_code);

    if (::getsockopt(s->backend, SOL_SOCKET, SO_ERROR,
                     &error_code, &length) < 0 || error_code != 0) {
        pool_.connectFailed(s->backend_index);
        ++metrics_.connect_failures;
        closeSession(s);
        return;
    }

    s->connecting = false;
    s->last = std::chrono::steady_clock::now();
    pool_.connectSucceeded(s->backend_index);
    propagateHalfCloses(s);
    if (!s->closed) update(s);
}

LoadBalancer::ReadResult LoadBalancer::readInto(
    int fd,
    Buffer& out,
    std::atomic<std::uint64_t>& metric) {
    std::array<char, 65536> buffer{};

    for (;;) {
        const ssize_t count = ::recv(fd, buffer.data(), buffer.size(), 0);

        if (count > 0) {
            const auto size = static_cast<std::size_t>(count);
            out.append(buffer.data(), size);
            metric += static_cast<std::uint64_t>(size);
            continue;
        }
        if (count == 0) return ReadResult::EndOfStream;
        if (errno == EAGAIN || errno == EWOULDBLOCK) return ReadResult::Open;
        if (errno == EINTR) continue;
        return ReadResult::Error;
    }
}

bool LoadBalancer::flush(int fd, Buffer& in) {
    while (!in.empty()) {
#ifdef MSG_NOSIGNAL
        const int flags = MSG_NOSIGNAL;
#else
        const int flags = 0;
#endif
        const ssize_t count = ::send(fd, in.begin(), in.size(), flags);
        if (count > 0) {
            in.consume(static_cast<std::size_t>(count));
            continue;
        }
        if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            return true;
        }
        if (count < 0 && errno == EINTR) continue;
        return false;
    }
    return true;
}

void LoadBalancer::propagateHalfCloses(
    const std::shared_ptr<Session>& s) {
    if (s->closed) return;

    // Once the client has sent EOF and all request bytes have reached the
    // backend, tell the backend that no more request bytes are coming.
    if (s->client_read_closed && s->c2b.empty() && !s->connecting &&
        !s->backend_write_shutdown) {
        if (::shutdown(s->backend, SHUT_WR) == 0 || errno == ENOTCONN) {
            s->backend_write_shutdown = true;
        } else {
            closeSession(s);
            return;
        }
    }

    // Once the backend has sent EOF and its response buffer is empty, tell
    // the client that no more response bytes are coming.
    if (s->backend_read_closed && s->b2c.empty() &&
        !s->client_write_shutdown) {
        if (::shutdown(s->client, SHUT_WR) == 0 || errno == ENOTCONN) {
            s->client_write_shutdown = true;
        } else {
            closeSession(s);
        }
    }
}

bool LoadBalancer::finished(const std::shared_ptr<Session>& s) const {
    return s->client_read_closed && s->backend_read_closed &&
           s->c2b.empty() && s->b2c.empty();
}

void LoadBalancer::update(const std::shared_ptr<Session>& s) {
    if (s->closed) return;

    std::uint32_t client_mask = EV_NONE;
    if (!s->client_read_closed && !s->pause_client) {
        client_mask |= EV_READ;
    }
    if (!s->b2c.empty() && !s->client_write_shutdown) {
        client_mask |= EV_WRITE;
    }

    std::uint32_t backend_mask = EV_NONE;
    if (s->connecting) {
        backend_mask |= EV_WRITE;
    } else {
        if (!s->backend_read_closed && !s->pause_backend) {
            backend_mask |= EV_READ;
        }
        if (!s->c2b.empty() && !s->backend_write_shutdown) {
            backend_mask |= EV_WRITE;
        }
    }

    loop_.modify(s->client, client_mask);
    loop_.modify(s->backend, backend_mask);
}

void LoadBalancer::closeSession(const std::shared_ptr<Session>& s) {
    if (!s || s->closed) return;
    s->closed = true; loop_.remove(s->client); loop_.remove(s->backend);
    refs_.erase(s->client); refs_.erase(s->backend); closeFd(s->client); closeFd(s->backend);
    pool_.closed(s->backend_index); ++metrics_.closed; --metrics_.active;
}

void LoadBalancer::sweep() {
    auto now = std::chrono::steady_clock::now();
    for (auto& s : sessions_) if (!s->closed) {
        if (s->connecting && now - s->created > cfg_.connect_timeout) {
            pool_.connectFailed(s->backend_index); ++metrics_.connect_failures; closeSession(s);
        } else if (now - s->last > cfg_.idle_timeout) {
            ++metrics_.idle_timeouts; closeSession(s);
        }
    }
    sessions_.erase(std::remove_if(sessions_.begin(), sessions_.end(), [](auto& s){ return s->closed; }), sessions_.end());
}

AdminServer::AdminServer(std::string h, std::string p, BackendPool& pool, const Metrics& m)
    : host_(std::move(h)), port_(std::move(p)), pool_(pool), metrics_(m) {}
AdminServer::~AdminServer() { stop(); }
void AdminServer::start() { if (running_.exchange(true)) return; thread_ = std::thread(&AdminServer::run, this); }
void AdminServer::stop() {
    if (!running_.exchange(false)) return;
    closeFd(listener_); listener_ = -1;
    if (thread_.joinable()) thread_.join();
}

std::string AdminServer::metricsText() const {
    std::ostringstream o;
    o << "lb_connections_accepted_total " << metrics_.accepted.load() << '\n'
      << "lb_connections_active " << metrics_.active.load() << '\n'
      << "lb_connections_closed_total " << metrics_.closed.load() << '\n'
      << "lb_connections_rejected_total " << metrics_.rejected.load() << '\n'
      << "lb_backend_connect_failures_total " << metrics_.connect_failures.load() << '\n'
      << "lb_bytes_client_to_backend_total " << metrics_.bytes_c2b.load() << '\n'
      << "lb_bytes_backend_to_client_total " << metrics_.bytes_b2c.load() << '\n'
      << "lb_backpressure_pauses_total " << metrics_.backpressure_pauses.load() << '\n'
      << "lb_idle_timeouts_total " << metrics_.idle_timeouts.load() << '\n';
    for (const auto& b : pool_.snapshot()) {
        const std::string labels = "backend=\"" + b.config.name + "\",address=\"" + b.config.host + ":" + b.config.port + "\"";
        o << "lb_backend_healthy{" << labels << "} " << (b.healthy ? 1 : 0) << '\n'
          << "lb_backend_active_connections{" << labels << "} " << b.active << '\n'
          << "lb_backend_connections_total{" << labels << "} " << b.total << '\n'
          << "lb_backend_failures_total{" << labels << "} " << b.failures << '\n'
          << "lb_backend_state{" << labels << ",mode=\"" << modeName(b.mode) << "\",circuit=\"" << circuitName(b.circuit) << "\"} 1\n";
    }
    return o.str();
}

std::string AdminServer::handleRequest(const std::string& req) {
    if (req.rfind("GET /metrics ", 0) == 0) return metricsText();
    std::istringstream in(req); std::string method, path; in >> method >> path;
    if (method == "POST") {
        const std::string prefix = "/backends/";
        if (path.rfind(prefix, 0) == 0) {
            auto slash = path.find('/', prefix.size());
            if (slash != std::string::npos) {
                try {
                    std::size_t id = std::stoull(path.substr(prefix.size(), slash - prefix.size()));
                    std::string action = path.substr(slash + 1);
                    bool ok = action == "drain" ? pool_.drain(id) : action == "activate" ? pool_.activate(id) : action == "disable" ? pool_.disable(id) : false;
                    return ok ? "ok\n" : "invalid backend or action\n";
                } catch (...) {}
            }
        }
    }
    return "not found\n";
}

void AdminServer::run() {
    try {
        auto a = resolve(host_, port_);
        listener_ = ::socket(a.addr.ss_family, SOCK_STREAM, 0);
        if (listener_ < 0) throw syserr("admin socket");
        int one = 1; ::setsockopt(listener_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        if (::bind(listener_, reinterpret_cast<sockaddr*>(&a.addr), a.len) < 0 || ::listen(listener_, 64) < 0) throw syserr("admin bind/listen");
        while (running_) {
            int c = ::accept(listener_, nullptr, nullptr);
            if (c < 0) { if (!running_) break; if (errno == EINTR) continue; else continue; }
            char buf[2048]{}; ssize_t n = ::recv(c, buf, sizeof(buf) - 1, 0);
            std::string body = handleRequest(n > 0 ? std::string(buf, static_cast<std::size_t>(n)) : "");
            std::string status = body == "not found\n" ? "404 Not Found" : "200 OK";
            std::string type = body.find("lb_") == 0 ? "text/plain; version=0.0.4" : "text/plain";
            std::string response = "HTTP/1.1 " + status + "\r\nContent-Type: " + type + "\r\nContent-Length: " + std::to_string(body.size()) + "\r\nConnection: close\r\n\r\n" + body;
            ::send(c, response.data(), response.size(), 0); closeFd(c);
        }
    } catch (const std::exception& e) { std::cerr << "admin server error: " << e.what() << '\n'; }
}

} // namespace lb
