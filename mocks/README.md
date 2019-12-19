# Mocks

This directory contains mocks for testing. The mocks are generated from
interfaces defined in the Go source files listed below.

Do not edit the go files in this directory. If you need to change them,
edit the interface definitions in the source files and then regenerate
the mocks using the commands below.

If the mockery binary is not in your path, use `$GOPATH/bin/mockery`.

The commands below generate mock files and leave them in the mocks directory.

## Minio Client Mock

The file `network/minio_client.go` defines the Minio client interface.
You can regenerate mocks from that interface by running:

`mockery -dir network -name MinioClientInterface`

## NSQ Client Mock

`mockery -dir network -name NSQClientInterface`

## Pharos Client Mock

`mockery -dir network -name PharosClientInterface`

## Redis Client Mock

The RedisClientInterface is defined in `network/redis_client.go`.
To regenerate the mocks, run:

`mockery -dir netword -name RedisClientInterface`
