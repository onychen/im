FROM node:24-alpine AS frontend-builder
WORKDIR /src/frontend

COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

FROM golang:1.24-alpine AS backend-builder
WORKDIR /src/backend

RUN apk add --no-cache ca-certificates git
COPY backend/go.mod backend/go.sum ./
RUN go mod download
COPY backend/ ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/goim-server ./cmd/server

FROM alpine:3.21
WORKDIR /app

RUN apk add --no-cache ca-certificates curl tzdata \
    && addgroup -S goim \
    && adduser -S -G goim goim \
    && mkdir -p uploads frontend/dist

COPY --from=backend-builder /out/goim-server ./goim-server
COPY backend/configs/config.docker.yaml ./configs/config.yaml
COPY --from=frontend-builder /src/frontend/dist ./frontend/dist
RUN chown -R goim:goim /app

USER goim
EXPOSE 8080

HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
    CMD curl --fail --silent http://localhost:8080/health || exit 1

ENTRYPOINT ["./goim-server", "-c", "configs/config.yaml"]
