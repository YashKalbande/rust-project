# Stage 1: The Builder
# Use Rust with MUSL support
FROM rust:1.82-slim-bullseye as builder

# Install MUSL tools
RUN apt-get update && apt-get install -y musl-tools && \
    rustup target add x86_64-unknown-linux-musl

# Set the working directory
WORKDIR /app

# Copy dependency files first (for build cache)
COPY Cargo.toml Cargo.lock ./

# Build dependencies with a dummy main.rs
RUN mkdir -p src/
RUN echo "fn main() { println!(\"Hello World\"); }" > src/main.rs
RUN cargo build --release --target x86_64-unknown-linux-musl

# Copy actual source code
COPY . .

# Rebuild final release binary with MUSL target
RUN cargo build --release --target x86_64-unknown-linux-musl

# Stage 2: The Runtime
# Use a minimal base image (no glibc needed)
FROM alpine:3.20

# Copy the statically linked binary
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/rust_query_service /usr/local/bin/

# Ensure it’s executable
RUN chmod +x /usr/local/bin/rust_query_service

# Set default command
CMD ["/usr/local/bin/rust_query_service"]
