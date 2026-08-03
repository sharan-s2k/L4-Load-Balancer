#pragma once
#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <string>
#include <utility>
namespace lb {
inline void closeFd(int fd){ if(fd>=0) ::close(fd); }
inline void setNonBlocking(int fd){ int f=::fcntl(fd,F_GETFL,0); if(f<0||::fcntl(fd,F_SETFL,f|O_NONBLOCK)<0) throw std::runtime_error(std::strerror(errno)); }
inline std::pair<std::string,std::string> splitHostPort(const std::string& s){ auto p=s.rfind(':'); if(p==std::string::npos||p==0||p+1>=s.size()) throw std::runtime_error("expected HOST:PORT: "+s); return {s.substr(0,p),s.substr(p+1)}; }
struct ResolvedAddress{ sockaddr_storage addr{}; socklen_t len{}; };
inline ResolvedAddress resolve(const std::string& host,const std::string& port,bool passive=false){ addrinfo h{}; h.ai_family=AF_UNSPEC; h.ai_socktype=SOCK_STREAM; if(passive) h.ai_flags=AI_PASSIVE; addrinfo* r=nullptr; int rc=::getaddrinfo(passive?nullptr:host.c_str(),port.c_str(),&h,&r); if(rc!=0||!r) throw std::runtime_error(std::string("getaddrinfo: ")+::gai_strerror(rc)); ResolvedAddress out; std::memcpy(&out.addr,r->ai_addr,r->ai_addrlen); out.len=static_cast<socklen_t>(r->ai_addrlen); ::freeaddrinfo(r); return out; }
}
