FROM golang:latest
ENV GOBIN /go/bin
ADD ./src /go/src/span-collector
WORKDIR /go/src/span-collector
RUN go get .
RUN go install .
CMD /go/bin/span-collector
