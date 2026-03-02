FROM golang:1.21-alpine AS builder

WORKDIR /app
COPY go.mod ./
RUN go mod download 2>/dev/null; go mod tidy
COPY . .
RUN CGO_ENABLED=0 go build -o /test-runner .

FROM alpine:3.19
COPY --from=builder /test-runner /test-runner
ENTRYPOINT ["/test-runner"]
