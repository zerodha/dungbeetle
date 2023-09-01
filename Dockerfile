FROM golang:1.12-alpine AS builder
RUN apk update && apk add make git
WORKDIR /dungbeetle/
COPY ./ ./
ENV CGO_ENABLED=0 GOOS=linux
RUN make build

FROM alpine:latest AS deploy
RUN apk --no-cache add ca-certificates
COPY --from=builder /dungbeetle/dungbeetle ./
COPY --from=builder /dungbeetle/sql ./
RUN mkdir -p /opt/config
COPY --from=builder /dungbeetle/config.toml.sample /opt/config/dungbeetle.toml

VOLUME ["/opt/config/"]

CMD ["./dungbeetle", "--config", "/opt/config/dungbeetle.toml"]
