FROM golang:1.23-bullseye AS build
ARG TARGETARCH
ARG CLICKHOUSE_BINARY_URL="https://builds.clickhouse.com/master/${TARGETARCH}/clickhouse"

WORKDIR /code

RUN curl "${CLICKHOUSE_BINARY_URL}" -o /usr/local/bin/clickhouse && chmod +x /usr/local/bin/clickhouse

# ENV CGO_ENABLED=1
# COPY . .
# RUN --mount=type=cache,target=/go/pkg/mod go mod download -x
# RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build make


# FROM debian:bullseye
# LABEL org.opencontainers.image.source=https://github.com/agnosticeng/agt
# ARG CLICKHOUSE_VERSION

# COPY --from=build /code/bin/* /
# COPY --from=build /usr/local/bin/clickhouse /usr/local/bin/clickhouse
# RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates
# RUN update-ca-certificates

# ENTRYPOINT ["/agt"]