FROM samuelololol/ubuntu-codeviz-base

RUN apt-get update && apt-get install -y git vim build-essential automake wget libtool cmake python
RUN cd /opt && git clone https://github.com/JiYou/phxpaxos.git && cd phxpaxos && \
    git submodule update --init --recursive && \
    cd third_party && \
    ./autoinstall.sh
RUN cd /opt/phxpaxos && ./autoinstall.sh && make && make install
RUN cd /opt/phxpaxos && cd plugin && make && make install
RUN cd /opt/phxpaxos && cd sample && make
