# hse-cloud-2020-hw-6-logbroker-solve

## Setup ClickHouse

`docker run -d --name some-clickhouse-server --ulimit nofile=262144:262144 -p 8123:8123 -p 9000:9000 yandex/clickhouse-server`

## Setup PostgreSQL

`docker run -p 5432:5432 -e POSTGRES_PASSWORD=password1 -e POSTGRES_USER=postgres -e POSTGRES_DB=logs -v pgdata:/var/lib/postgresql/data -d postgres:9.3.6`

## Setup logbroker

`docker build . -t logbroker:latest`
`docker run -d -e LOGBROKER_PG_HOST=pg-vm-hostname -p 8000:8000 logbroker`

## Setup worker

`docker build . -t worker:latest`
`docker run -d -e LOGBROKER_PG_HOST=pg-vm-hostname -e LOGBROKER_CH_HOST=ch-vm-hostname worker`