FROM golang as builder
ADD ./error-recorder/src /go/src/error-recorder
ADD ./shared /go/src/shared
WORKDIR /go/src/error-recorder
RUN go get .
ENV GOBIN /go/bin
ENV GOOS linux
ENV CGO_ENABLED 0
ENV GOARCH amd64
RUN go install .

FROM alpine
WORKDIR /root/
COPY --from=builder /go/bin/error-recorder /root/
CMD ["./error-recorder"]
