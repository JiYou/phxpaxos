FROM ubuntu:14.04

RUN apt-get update && apt-get install -y git vim build-essential automake wget libtool python wget \
    autoconf libgflags-dev libgtest-dev clang libc++-dev curl libssl-dev 
RUN cd /opt && git clone -b $(curl -L https://grpc.io/release) https://github.com/grpc/grpc
RUN cd /opt/ && cd grpc && git checkout v1.1.0 && \
    git submodule update --init && \
    make -j16 && make install
RUN cd /usr &&  \
    wget https://cmake.org/files/v3.13/cmake-3.13.0-rc1-Linux-x86_64.sh && \
    chmod +x cmake-3.13.0-rc1-Linux-x86_64.sh && \
    ./cmake-3.13.0-rc1-Linux-x86_64.sh --prefix=/usr --skip-license
RUN cd /opt && git clone https://github.com/JiYou/phxpaxos.git && cd phxpaxos && \
    git submodule update --init --recursive && \
    cd third_party && \
    ./autoinstall.sh
RUN cd /opt/phxpaxos && ./autoinstall.sh && make && make install
RUN cd /opt/phxpaxos && cd plugin && make && make install
RUN cd /opt/phxpaxos/ &&  cd third_party/ && \
    mkdir -p grpc/lib && \
    cp -rf /usr/local/lib/libgrpc* ./
RUN cd /opt/phxpaxos && cd sample && make
