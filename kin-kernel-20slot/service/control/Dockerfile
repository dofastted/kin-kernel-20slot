FROM golang:1.27.0-bookworm AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go test ./... && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/kin-control ./cmd/kin-control

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/kin-control /usr/local/bin/kin-control
EXPOSE 9090
ENTRYPOINT ["/usr/local/bin/kin-control"]

