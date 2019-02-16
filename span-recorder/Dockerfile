FROM golang as builder
ADD ./span-recorder/src /go/src/span-recorder
ADD ./shared /go/src/shared
WORKDIR /go/src/span-recorder
RUN go get .
ENV GOBIN /go/bin
ENV GOOS linux
ENV CGO_ENABLED 0
ENV GOARCH amd64
RUN go install .

FROM alpine
WORKDIR /root/
COPY --from=builder /go/bin/span-recorder /root/
CMD ["./span-recorder"]
