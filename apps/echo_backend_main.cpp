#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>
#include <array>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
static void serve(int fd,std::string name){
    std::array<char,65536>b{};
    for(;;){
        ssize_t n=::recv(fd,b.data(),b.size(),0);
        if(n<=0)break;
        std::string prefix=name+":";
        if(::send(fd,prefix.data(),prefix.size(),0)<0)break;
        std::size_t sent=0;
        while(sent<static_cast<std::size_t>(n)){
            ssize_t x=::send(fd,b.data()+sent,static_cast<std::size_t>(n)-sent,0);
            if(x<=0){::close(fd);return;}
            sent+=static_cast<std::size_t>(x);
        }
    }
    ::close(fd);
}
int main(int argc,char** argv){
    if(argc!=3){std::cerr<<"echo-backend PORT NAME\n";return 1;}
    addrinfo h{};
    h.ai_family=AF_INET;
    h.ai_socktype=SOCK_STREAM;
    h.ai_flags=AI_PASSIVE;
    addrinfo* r=nullptr;
    int rc=::getaddrinfo(nullptr,argv[1],&h,&r);
    if(rc!=0||!r){std::cerr<<::gai_strerror(rc)<<'\n';return 1;}
    int fd=::socket(r->ai_family,r->ai_socktype,r->ai_protocol);
    int one=1;
    ::setsockopt(fd,SOL_SOCKET,SO_REUSEADDR,&one,sizeof(one));
    if(fd<0||::bind(fd,r->ai_addr,r->ai_addrlen)<0||::listen(fd,SOMAXCONN)<0){
        std::cerr<<"bind/listen: "<<std::strerror(errno)<<'\n';
        return 1;
    }
    ::freeaddrinfo(r);
    std::cout<<argv[2]<<" listening on "<<argv[1]<<'\n';
    for(;;){
        int c=::accept(fd,nullptr,nullptr);
        if(c<0){if(errno==EINTR)continue;break;}
        std::thread(serve,c,std::string(argv[2])).detach();
    }
    ::close(fd);
}
