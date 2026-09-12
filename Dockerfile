# syntax=docker/dockerfile:1

FROM golang:1.22-alpine AS build
WORKDIR /src

COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal

ARG VERSION=dev
RUN CGO_ENABLED=0 go build \
    -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/sse-relay \
    ./cmd/sse-relay

# Static binary, no libc, no shell: distroless keeps the image to just the
# binary and CA certs, and runs as the built-in nonroot user.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/sse-relay /sse-relay

EXPOSE 8080
ENTRYPOINT ["/sse-relay"]
CMD ["-addr", ":8080"]
