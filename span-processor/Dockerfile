FROM shared as builder
ADD ./span-processor/src /go/src/span-processor
WORKDIR /go/src/span-processor
RUN go install .

FROM alpine
RUN apk update && apk add ca-certificates
WORKDIR /root/
COPY --from=builder /go/bin/span-processor /root/
CMD ["./span-processor"]
