# One Dockerfile for every Calligraphy binary, selected at build time:
#
#   docker build -f deploy/docker/Dockerfile --build-arg BINARY=calligraphy-api .
#
# Multi-stage: the build stage carries the whole toolchain; the runtime
# image is FROM scratch and carries exactly three things -- the static
# binary, CA certificates (http.callback speaks TLS to the outside), and
# tzdata is deliberately NOT one of them (everything in Calligraphy is UTC).
# Result: ~15MB images with no shell, no package manager, and no CVE
# surface beyond the binary itself.
#
# The scratch base has one operational consequence worth knowing before it
# bites: there is no shell for `docker exec` debugging and no wget for a
# compose healthcheck. That's a trade taken knowingly -- Kubernetes probes
# (deploy/k8s) use httpGet, which needs nothing inside the container, and
# compose gates startup ordering on Postgres/Redis health instead (the
# binaries retry their dependencies anyway).

FROM golang:1.25-alpine AS build

WORKDIR /src

# Dependencies as their own layer: go.mod/go.sum change far less often
# than source, so day-to-day rebuilds skip the download entirely.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG BINARY=calligraphy-api
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${BINARY}

FROM scratch

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/app /app

# Numeric because scratch has no /etc/passwd to name a user in.
USER 65534:65534

ENTRYPOINT ["/app"]
