# docker build -t pgasus .

# see https://link.medium.com/Ra2kvVysZ7
# investigate https://github.com/GoogleContainerTools/distroless later

#FROM golang:1.6.4-alpine AS builder
FROM golang:1.14.4-alpine3.12 AS builder
RUN apk --no-cache add ca-certificates

COPY . /go/src/github.com/debackerl/pgasus
WORKDIR /go/src/github.com/debackerl/pgasus
#RUN ls -lh . && go version && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -installsuffix cgo -ldflags="-w -s" -o /bin/pgasus . && ls -lh /bin/pgasus
RUN ls -lh . && go version && GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o /bin/pgasus . && ls -lh /bin/pgasus

RUN addgroup -S pgasus -g 1001 && \
    adduser -S pgasus -u 1001 -g pgasus

FROM scratch

WORKDIR /
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /etc/group /etc/group
COPY --from=builder /bin/pgasus /bin/pgasus

USER pgasus

CMD ["/bin/pgasus"]
