FROM golang as builder
WORKDIR /go
RUN go get github.com/gocql/gocql
RUN go get github.com/segmentio/kafka-go
RUN go get github.com/gorilla/mux
RUN go get github.com/satori/go.uuid
ENV GOBIN /go/bin
ENV GOOS linux
ENV CGO_ENABLED 0
ENV GOARCH amd64
ADD ./shared /go/src/shared
