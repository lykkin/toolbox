FROM shared as builder
ADD ./span-recorder/src /go/src/span-recorder
WORKDIR /go/src/span-recorder
RUN go install .

FROM alpine
WORKDIR /root/
COPY --from=builder /go/bin/span-recorder /root/
CMD ["./span-recorder"]
