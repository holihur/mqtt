# 阶段 1: 构建 dashboard 前端
FROM node:24-alpine AS webui
WORKDIR /app
COPY web/dashboard/package.json web/dashboard/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY web/dashboard/ ./
RUN npm run build

# 阶段 2: 编译 broker，嵌入前端产物
FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# 用刚构建的前端覆盖嵌入目录 (占位 index.html 会被替换)
COPY --from=webui /app/dist ./internal/webui/dist
RUN CGO_ENABLED=0 go build -o /bin/broker ./cmd/broker

# 阶段 3: 运行时镜像
FROM alpine:3.22
COPY --from=builder /bin/broker /bin/broker
EXPOSE 1883 8083 8080
ENTRYPOINT ["/bin/broker"]
