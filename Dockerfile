FROM golang:1.23 AS build

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -installsuffix 'static' ./cmd/metjson2db

FROM gcr.io/distroless/static

WORKDIR /app

COPY --from=build /app/metjson2db /app/

EXPOSE 8080

USER nonroot:nonroot

ENTRYPOINT [ "/app/metjson2db" ]
