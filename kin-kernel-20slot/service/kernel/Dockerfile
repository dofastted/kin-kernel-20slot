FROM rust:1.98.0-bookworm AS build
WORKDIR /src
COPY Cargo.toml ./
COPY src ./src
RUN cargo build --release

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /src/target/release/kin-kernel /usr/local/bin/kin-kernel
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/kin-kernel"]
