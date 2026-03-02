FROM golang:1.21-alpine AS builder

RUN apk add --no-cache git

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /test-runner .

FROM alpine:3.19
COPY --from=builder /test-runner /test-runner
ENTRYPOINT ["/test-runner"]
