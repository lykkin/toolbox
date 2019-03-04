FROM shared as builder
ADD ./error-recorder/src /go/src/error-recorder
WORKDIR /go/src/error-recorder
RUN go install .

FROM alpine
WORKDIR /root/
COPY --from=builder /go/bin/error-recorder /root/
CMD ["./error-recorder"]
