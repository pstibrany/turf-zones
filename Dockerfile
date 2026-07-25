# modernc.org/sqlite is pure Go, so CGO stays off and the result is a static
# binary that runs on a scratch-like base.
FROM golang:1.26-alpine AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

# Everything go:embed refers to has to be here too, or the build fails with
# "pattern ...: no matching files found". Copying named paths rather than the
# whole tree keeps the local database and the token file out of the image, so
# add to this line when adding embedded assets.
COPY *.go index.html ./
COPY templates ./templates

RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /turf-exporter .

FROM alpine:3.21
RUN apk add --no-cache ca-certificates tzdata && \
    adduser -D -u 10001 turf
COPY --from=build /turf-exporter /usr/local/bin/turf-exporter

# The volume is mounted here; see fly.toml.
RUN mkdir -p /data && chown turf:turf /data
USER turf
VOLUME /data
# 8080 is published by fly.toml; 9090 carries telemetry and must not be.
EXPOSE 8080 9090

ENTRYPOINT ["/usr/local/bin/turf-exporter"]
