#pragma once
#include <cstdint>
#include <vector>
namespace lb {
enum EventMask:std::uint32_t{
    EV_NONE=0,
    EV_READ=1,
    EV_WRITE=2,
    EV_ERROR=4
};
struct ReadyEvent{
    int fd;
    std::uint32_t mask;
};
class EventLoop{
public:
    EventLoop();
    ~EventLoop();
    void add(int,std::uint32_t);
    void modify(int,std::uint32_t);
    void remove(int);
    std::vector<ReadyEvent> wait(int,int=1024);
private:
    int fd_{-1};
};
}
