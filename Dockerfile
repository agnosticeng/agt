FROM golang:1.23-bullseye AS build
ARG TARGETARCH
ARG CLICKHOUSE_VERSION

WORKDIR /code

RUN wget https://github.com/ClickHouse/ClickHouse/releases/download/v${CLICKHOUSE_VERSION}-stable/clickhouse-common-static-${CLICKHOUSE_VERSION}-${TARGETARCH}.tgz
RUN tar xvzf clickhouse-common-static-${CLICKHOUSE_VERSION}-${TARGETARCH}.tgz

ENV CGO_ENABLED=1
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod go mod download -x
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build make


FROM debian:bullseye
LABEL org.opencontainers.image.source=https://github.com/agnosticeng/agt
ARG CLICKHOUSE_VERSION

COPY --from=build /code/bin/* /
COPY --from=build /code/clickhouse-common-static-${CLICKHOUSE_VERSION}/usr/bin/clickhouse /usr/bin/clickhouse

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates
RUN update-ca-certificates

ENTRYPOINT ["/agt"]