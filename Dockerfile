FROM rust:1-alpine3.17

WORKDIR /usr/src/ssache

COPY . .

RUN apk add --no-cache musl-dev

RUN rustup target add x86_64-unknown-linux-musl

RUN cargo build --release --target x86_64-unknown-linux-musl

RUN cargo install --path . --bin ssache

ENV RUST_LOG=info

CMD ["ssache"]
