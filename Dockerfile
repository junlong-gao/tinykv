FROM golang:1.13.8

RUN apt-get update && apt-get install -y wget zip \
    && wget https://github.com/protocolbuffers/protobuf/releases/download/v3.12.3/protoc-3.12.3-linux-x86_64.zip \
    && unzip protoc-3.12.3-linux-x86_64.zip \
    && cp bin/protoc /usr/local/bin

RUN mkdir -p /src
COPY kv /src/kv
COPY log /src/log
COPY proto /src/proto
COPY raft /src/raft
COPY go.mod /src/go.mod
COPY go.sum /src/go.sum
COPY Makefile /src/Makefile
COPY scheduler /src/scheduler

WORKDIR /src/
ENTRYPOINT [ "make", "test" ]
