FROM golang:1.24.2-bookworm AS builder
WORKDIR /src
RUN git init && git remote add origin https://github.com/minio/minio.git \
    && git fetch --depth 1 origin 0d7408fc9969caf07de6a8c3a84f9fbb10a6739e \
    && git checkout --detach FETCH_HEAD
ENV CGO_ENABLED=0 GOMAXPROCS=2
RUN go build -p 2 -trimpath -ldflags="-s -w -X github.com/minio/minio/cmd.Version=RELEASE.2025-04-22T22-12-26Z -X github.com/minio/minio/cmd.ReleaseTag=RELEASE.2025-04-22T22-12-26Z -X github.com/minio/minio/cmd.CommitID=0d7408fc9969caf07de6a8c3a84f9fbb10a6739e" -o /minio .
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /minio /usr/local/bin/minio
USER 1000:1000
ENTRYPOINT ["minio"]
