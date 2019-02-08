FROM golang as builder
ADD ./metric-processor/src /go/src/metric-processor
ADD ./shared /go/src/shared
WORKDIR /go/src/metric-processor
RUN go get .
ENV GOBIN /go/bin
ENV GOOS linux
ENV CGO_ENABLED 0
ENV GOARCH amd64
RUN go install .

FROM alpine
RUN apk update && apk add ca-certificates
WORKDIR /root/
COPY --from=builder /go/bin/metric-processor /root/
COPY ./metric-processor/src/whitelist.json /conf/
CMD ["./metric-processor"]
