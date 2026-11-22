FROM rust:1.100.0-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY pgo ./pgo
ENV RUSTFLAGS="-C target-cpu=cascadelake -C target-feature=+crt-static -Cprofile-use=/src/pgo/merged.profdata"
RUN cargo build --release --locked --target x86_64-unknown-linux-gnu \
 && cp target/x86_64-unknown-linux-gnu/release/pipeline /pipeline \
 && chmod 0755 /pipeline

FROM build AS portable
RUN RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+crt-static" cargo build --release --locked --target x86_64-unknown-linux-gnu --target-dir /t3 \
 && cp /t3/x86_64-unknown-linux-gnu/release/pipeline /pipeline-v3 \
 && RUSTFLAGS="-C target-cpu=x86-64-v2 -C target-feature=+crt-static" cargo build --release --locked --target x86_64-unknown-linux-gnu --target-dir /t2 \
 && cp /t2/x86_64-unknown-linux-gnu/release/pipeline /pipeline-v2 \
 && chmod 0755 /pipeline-v3 /pipeline-v2

FROM scratch
COPY --from=build /pipeline /pipeline
COPY --from=portable /pipeline-v3 /pipeline-v3
COPY --from=portable /pipeline-v2 /pipeline-v2
CMD ["/pipeline"]
