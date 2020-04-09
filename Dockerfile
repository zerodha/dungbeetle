FROM golang:1.12-alpine AS builder
RUN apk update && apk add make git
WORKDIR /sql-jobber/
COPY ./ ./
ENV CGO_ENABLED=0 GOOS=linux
RUN make build

FROM alpine:latest AS deploy
RUN apk --no-cache add ca-certificates
COPY --from=builder /sql-jobber/sql-jobber ./
COPY --from=builder /sql-jobber/sql ./
RUN mkdir -p /opt/config
COPY --from=builder /sql-jobber/config.toml.sample /opt/config/sql-jobber.toml

VOLUME ["/opt/config/"]

CMD ["./sql-jobber", "--config", "/opt/config/sql-jobber.toml"]
