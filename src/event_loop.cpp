#include "event_loop.h"

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <string>
#include <unistd.h>

#if defined(LB_USE_EPOLL)
#include <sys/epoll.h>
#elif defined(LB_USE_KQUEUE)
#include <sys/event.h>
#endif

namespace lb {

namespace {

std::runtime_error error(const char* message) {
    return std::runtime_error(
        std::string(message) + ": " + std::strerror(errno));
}

}  // namespace

EventLoop::EventLoop() {
#if defined(LB_USE_EPOLL)
    fd_ = ::epoll_create1(EPOLL_CLOEXEC);
#else
    fd_ = ::kqueue();
#endif
    if (fd_ < 0) {
        throw error("event loop create failed");
    }
}

EventLoop::~EventLoop() {
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

void EventLoop::add(int fd, std::uint32_t mask) {
#if defined(LB_USE_EPOLL)
    epoll_event event{};
    event.data.fd = fd;
    event.events = EPOLLERR | EPOLLHUP | EPOLLRDHUP |
                   ((mask & EV_READ) ? EPOLLIN : 0) |
                   ((mask & EV_WRITE) ? EPOLLOUT : 0);
    if (::epoll_ctl(fd_, EPOLL_CTL_ADD, fd, &event) < 0) {
        throw error("epoll add");
    }
#else
    struct kevent changes[2];
    EV_SET(&changes[0], fd, EVFILT_READ,
           EV_ADD | ((mask & EV_READ) ? EV_ENABLE : EV_DISABLE),
           0, 0, nullptr);
    EV_SET(&changes[1], fd, EVFILT_WRITE,
           EV_ADD | ((mask & EV_WRITE) ? EV_ENABLE : EV_DISABLE),
           0, 0, nullptr);
    if (::kevent(fd_, changes, 2, nullptr, 0, nullptr) < 0) {
        throw error("kqueue add");
    }
#endif
}

void EventLoop::modify(int fd, std::uint32_t mask) {
#if defined(LB_USE_EPOLL)
    epoll_event event{};
    event.data.fd = fd;
    event.events = EPOLLERR | EPOLLHUP | EPOLLRDHUP |
                   ((mask & EV_READ) ? EPOLLIN : 0) |
                   ((mask & EV_WRITE) ? EPOLLOUT : 0);
    if (::epoll_ctl(fd_, EPOLL_CTL_MOD, fd, &event) < 0) {
        throw error("epoll modify");
    }
#else
    struct kevent changes[2];
    EV_SET(&changes[0], fd, EVFILT_READ,
           (mask & EV_READ) ? EV_ENABLE : EV_DISABLE,
           0, 0, nullptr);
    EV_SET(&changes[1], fd, EVFILT_WRITE,
           (mask & EV_WRITE) ? EV_ENABLE : EV_DISABLE,
           0, 0, nullptr);
    if (::kevent(fd_, changes, 2, nullptr, 0, nullptr) < 0) {
        throw error("kqueue modify");
    }
#endif
}

void EventLoop::remove(int fd) {
#if defined(LB_USE_EPOLL)
    ::epoll_ctl(fd_, EPOLL_CTL_DEL, fd, nullptr);
#else
    struct kevent changes[2];
    EV_SET(&changes[0], fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
    EV_SET(&changes[1], fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
    ::kevent(fd_, changes, 2, nullptr, 0, nullptr);
#endif
}

std::vector<ReadyEvent> EventLoop::wait(int timeout_ms, int max_events) {
    std::vector<ReadyEvent> output;

#if defined(LB_USE_EPOLL)
    std::vector<epoll_event> events(static_cast<std::size_t>(max_events));
    const int count =
        ::epoll_wait(fd_, events.data(), max_events, timeout_ms);

    if (count < 0) {
        if (errno == EINTR) {
            return output;
        }
        throw error("epoll wait");
    }

    output.reserve(static_cast<std::size_t>(count));
    for (int i = 0; i < count; ++i) {
        std::uint32_t mask = EV_NONE;
        const auto native = events[i].events;

        if (native & EPOLLIN) {
            mask |= EV_READ;
        }
        if (native & EPOLLOUT) {
            mask |= EV_WRITE;
        }
        // A peer half-close is a readable EOF, not an immediate fatal error.
        if (native & (EPOLLRDHUP | EPOLLHUP)) {
            mask |= EV_READ;
        }
        if (native & EPOLLERR) {
            mask |= EV_ERROR;
        }

        output.push_back({events[i].data.fd, mask});
    }
#else
    std::vector<struct kevent> events(static_cast<std::size_t>(max_events));
    timespec timeout{
        timeout_ms / 1000,
        (timeout_ms % 1000) * 1'000'000,
    };

    const int count =
        ::kevent(fd_, nullptr, 0, events.data(), max_events, &timeout);

    if (count < 0) {
        if (errno == EINTR) {
            return output;
        }
        throw error("kevent wait");
    }

    output.reserve(static_cast<std::size_t>(count));
    for (int i = 0; i < count; ++i) {
        std::uint32_t mask = EV_NONE;

        if (events[i].filter == EVFILT_READ) {
            mask |= EV_READ;
        }
        if (events[i].filter == EVFILT_WRITE) {
            mask |= EV_WRITE;
        }
        // EV_EOF on a read filter must be consumed through recv() so buffered
        // bytes are forwarded before the opposite side is half-closed.
        if ((events[i].flags & EV_EOF) &&
            events[i].filter == EVFILT_READ) {
            mask |= EV_READ;
        }
        if (events[i].flags & EV_ERROR) {
            mask |= EV_ERROR;
        }

        output.push_back({static_cast<int>(events[i].ident), mask});
    }
#endif

    return output;
}

}  // namespace lb
