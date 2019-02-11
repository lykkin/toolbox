FROM golang as builder
ADD ./error-processor/src /go/src/error-processor
ADD ./shared /go/src/shared
WORKDIR /go/src/error-processor
RUN go get .
ENV GOBIN /go/bin
ENV GOOS linux
ENV CGO_ENABLED 0
ENV GOARCH amd64
RUN go install .

FROM alpine
WORKDIR /root/
COPY --from=builder /go/bin/error-processor /root/
CMD ["./error-processor"]
