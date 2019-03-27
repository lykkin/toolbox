FROM shared as builder
ADD ./span-collector/src /go/src/span-collector
WORKDIR /go/src/span-collector
RUN go install .

FROM alpine
WORKDIR /root/
COPY --from=builder /go/bin/span-collector /root/
CMD ["./span-collector"]
