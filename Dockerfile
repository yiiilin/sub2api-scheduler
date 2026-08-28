# Build stage
FROM golang:1.26-alpine AS build
WORKDIR /src

# Cache dependencies first
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/sub2api-scheduler .

# Runtime stage
FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata \
    && adduser -D -u 10001 scheduler
WORKDIR /app
COPY --from=build /out/sub2api-scheduler /usr/local/bin/sub2api-scheduler

USER scheduler
EXPOSE 8090

# Default: config at /app/config.yaml (mount it or set CONFIG_PATH)
ENV CONFIG_PATH=/app/config.yaml
CMD ["sub2api-scheduler"]
