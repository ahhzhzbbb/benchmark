FROM golang:1.24-alpine AS builder

WORKDIR /app

# Install git for downloading dependencies
RUN apk add --no-cache git

# Copy go.mod and go.sum files
COPY go.mod go.sum ./

# Download all dependencies
RUN go mod download

# Copy the source code
COPY . .

# Build the application
# CGO_ENABLED=0 ensures a statically linked binary
RUN CGO_ENABLED=0 GOOS=linux go build -o benchmarkDB .

# Use a minimal alpine image for the final stage
FROM alpine:latest

WORKDIR /root/

# Install CA certificates for TLS connections
RUN apk --no-cache add ca-certificates

# Copy the pre-built binary file from the previous stage
COPY --from=builder /app/benchmarkDB .

# Copy sample config file
COPY --from=builder /app/config.yaml .

# Command to run the executable
ENTRYPOINT ["./benchmarkDB"]

# Default: run with sample config
CMD ["--config", "config.yaml", "--mode", "load"]
