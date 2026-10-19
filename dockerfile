FROM golang:1.27.1 AS builder

WORKDIR /app

COPY go.mod go.sum   ./

RUN go mod download

COPY . .

RUN go build -o server ./cmd/server
RUN go build -o worker ./cmd/worker

#runtime stage 

FROM debian:bookworm-slim

WORKDIR /app

COPY --from=builder /app/server .
COPY --from=builder /app/worker .

EXPOSE 8080

CMD ["./server"]