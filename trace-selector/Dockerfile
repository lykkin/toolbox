FROM shared as builder
ADD ./trace-selector/src /go/src/trace-selector
WORKDIR /go/src/trace-selector
RUN go install .

FROM alpine
WORKDIR /root/
COPY --from=builder /go/bin/trace-selector /root/
CMD ["./trace-selector"]
