FROM golang:1.25 AS build

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -installsuffix 'static' -o sqsworker ./cmd/sqsworker

FROM gcr.io/distroless/static

WORKDIR /app

COPY --from=build /app/sqsworker /app/

USER nonroot:nonroot

ENTRYPOINT [ "/app/sqsworker" ]
