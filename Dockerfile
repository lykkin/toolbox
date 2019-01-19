FROM golang as builder
ADD ./src /go/src/span-collector
WORKDIR /go/src/span-collector
RUN go get .
ENV GOBIN /go/bin
ENV GOOS linux
ENV CGO_ENABLED 0
ENV GOARCH amd64
RUN go install .

FROM alpine
WORKDIR /root/
COPY --from=builder /go/bin/span-collector /root/
CMD ["./span-collector"]
