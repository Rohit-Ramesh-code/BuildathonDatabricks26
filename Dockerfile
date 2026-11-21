# linux/amd64 image for the Shopping Sessions scorer.
# Build has network; the run does not. The process is uid 1000.
FROM rust:1.85-bookworm AS build

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim

RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /src/target/release/shopping-sessions /app/shopping-sessions
RUN chmod 755 /app/shopping-sessions

USER 1000
WORKDIR /scratch
CMD ["/app/shopping-sessions"]
