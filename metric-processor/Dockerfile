FROM shared as builder
ADD ./metric-processor/src /go/src/metric-processor
WORKDIR /go/src/metric-processor
RUN go install .

FROM alpine
RUN apk update && apk add ca-certificates
WORKDIR /root/
COPY --from=builder /go/bin/metric-processor /root/
COPY ./metric-processor/conf /conf
CMD ["./metric-processor"]
