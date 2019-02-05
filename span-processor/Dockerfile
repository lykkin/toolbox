FROM golang as builder
ADD ./span-processor/src /go/src/span-processor
ADD ./shared /go/src/shared
WORKDIR /go/src/span-processor
RUN go get .
ENV GOBIN /go/bin
ENV GOOS linux
ENV CGO_ENABLED 0
ENV GOARCH amd64
RUN go install .

FROM alpine
RUN apk update && apk add ca-certificates
WORKDIR /root/
COPY --from=builder /go/bin/span-processor /root/
CMD ["./span-processor"]
