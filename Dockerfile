FROM golang:1.23 AS build

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -installsuffix 'static' ./cmd/metdatacb

FROM gcr.io/distroless/static

WORKDIR /app

COPY --from=build /app/metdatacb /app/

EXPOSE 8080

USER nonroot:nonroot

ENTRYPOINT [ "/app/metdatacb" ]
