module github.com/knadh/sql-jobber

go 1.12

require (
	cloud.google.com/go/pubsub v1.10.3 // indirect
	github.com/ClickHouse/clickhouse-go v1.4.5
	github.com/RichardKnop/machinery v1.10.5
	github.com/aws/aws-sdk-go v1.38.26 // indirect
	github.com/docker/go-connections v0.4.0
	github.com/go-chi/chi v4.1.2+incompatible
	github.com/go-redis/redis/v8 v8.8.2 // indirect
	github.com/go-redsync/redsync/v4 v4.3.0 // indirect
	github.com/go-sql-driver/mysql v1.6.0
	github.com/gomodule/redigo v2.0.0+incompatible
	github.com/klauspost/compress v1.12.1 // indirect
	github.com/knadh/goyesql v2.0.0+incompatible
	github.com/knadh/koanf v0.16.0
	github.com/lib/pq v1.10.1
	github.com/mitchellh/copystructure v1.1.2 // indirect
	github.com/mitchellh/mapstructure v1.4.1 // indirect
	github.com/mithrandie/csvq-driver v1.6.9
	github.com/satori/go.uuid v1.2.0
	github.com/sijms/go-ora/v2 v2.5.22
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.8.1
	github.com/testcontainers/testcontainers-go v0.17.0
	github.com/youmark/pkcs8 v0.0.0-20201027041543-1326539a0a0a // indirect
	go.mongodb.org/mongo-driver v1.5.1 // indirect
	go.opentelemetry.io/otel/internal/metric v0.27.0 // indirect
)

replace github.com/docker/docker => github.com/docker/docker v20.10.3-0.20221013203545-33ab36d6b304+incompatible // 22.06 branch
