FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /bin/broker ./cmd/broker

FROM alpine:3.19
COPY --from=builder /bin/broker /bin/broker
EXPOSE 1883 8083
ENTRYPOINT ["/bin/broker"]
