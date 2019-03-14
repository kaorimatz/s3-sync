FROM golang:1.12 as builder

WORKDIR $GOPATH/src/github.com/kaorimatz/s3-sync
COPY . .

RUN CGO_ENABLED=0 go build -ldflags '-extldflags -static -s -w' -o /s3-sync

FROM scratch
MAINTAINER Satoshi Matsumoto <kaorimatz@gmail.com>

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /tmp /tmp

COPY --from=builder /s3-sync /s3-sync

ENTRYPOINT ["/s3-sync"]
