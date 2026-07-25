# modernc.org/sqlite is pure Go, so CGO stays off and the result is a static
# binary that runs on a scratch-like base.
FROM golang:1.26-alpine AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /turf-exporter .

FROM alpine:3.21
RUN apk add --no-cache ca-certificates tzdata && \
    adduser -D -u 10001 turf
COPY --from=build /turf-exporter /usr/local/bin/turf-exporter

# The volume is mounted here; see fly.toml.
RUN mkdir -p /data && chown turf:turf /data
USER turf
VOLUME /data
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/turf-exporter"]
