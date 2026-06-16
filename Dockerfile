FROM golang:1.22-alpine AS builder
WORKDIR /src
COPY go.mod ./
COPY *.go ./
COPY index.html ./
RUN CGO_ENABLED=0 go build -o kbproxy .

FROM alpine:latest
RUN apk add --no-cache ca-certificates curl
COPY --from=builder /src/kbproxy /usr/local/bin/kbproxy
ENTRYPOINT ["kbproxy"]
