#include "load_balancer.h"
#include <cassert>
#include <chrono>
#include <iostream>

int main() {
    std::vector<lb::BackendConfig> backends{
        {"a", "127.0.0.1", "9001", 10},
        {"b", "127.0.0.1", "9002", 10}
    };
    lb::BackendPool pool(std::move(backends), lb::Algorithm::LeastConnections,
                         2, std::chrono::milliseconds(100));
    auto first = pool.choose("client"); assert(first.has_value());
    pool.opened(*first);
    auto second = pool.choose("client"); assert(second.has_value()); assert(*second != *first);
    assert(pool.drain(*first));
    auto third = pool.choose("client"); assert(third.has_value()); assert(*third != *first);
    pool.connectFailed(*third); pool.connectFailed(*third);
    auto snap = pool.snapshot(); assert(snap[*third].circuit == lb::CircuitState::Open);
    std::cout << "all tests passed\n";
}
