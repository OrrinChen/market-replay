# syntax=docker/dockerfile:1

FROM golang:1.22-bookworm AS build

WORKDIR /src
ARG GOPROXY=https://proxy.golang.org,direct
ARG GOSUMDB=sum.golang.org
ENV GOPROXY=$GOPROXY
ENV GOSUMDB=$GOSUMDB
COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/market-replay ./cmd/market-replay
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/server ./cmd/server
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -tags kafka -trimpath -ldflags="-s -w" -o /out/kafka-replay ./cmd/kafka-replay

FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app
COPY --from=build /out/market-replay /app/market-replay
COPY --from=build /out/server /app/server
COPY --from=build /out/worker /app/worker
COPY --from=build /out/kafka-replay /app/kafka-replay
COPY testdata /app/testdata
COPY web /app/web
COPY docs /app/docs

USER nonroot:nonroot
CMD ["/app/server"]
