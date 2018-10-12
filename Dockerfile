FROM samuelololol/ubuntu-codeviz-base

RUN apt-get update && apt-get install -y git vim build-essential automake wget libtool cmake python
RUN mv /usr/bin/gcc /usr/bin/gcc.back && \
    mv /usr/bin/g++ /usr/bin/g++.back && \
    ln -s /usr/local/gccgraph/bin/gcc /usr/bin/gcc && \
    ln -s /usr/local/gccgraph/bin/g++ /usr/bin/g++
RUN cd /opt && git clone https://github.com/JiYou/phxpaxos.git && cd phxpaxos && \
    git submodule update --init --recursive && \
    cd third_party && \
    ./autoinstall.sh
RUN cd /opt/phxpaxos && ./autoinstall.sh && make && make install
RUN cd /opt/phxpaxos && cd plugin && make && make install
RUN cd /opt/phxpaxos && cd sample && make
