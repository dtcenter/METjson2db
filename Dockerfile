FROM golang:1.23 AS build

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -installsuffix 'static' ./cmd/metdatacb-cli

FROM gcr.io/distroless/static

WORKDIR /app

COPY --from=build /app/metdatacb-cli /app/

EXPOSE 8080

USER nonroot:nonroot

ENTRYPOINT [ "/app/metdatacb-cli" ]
