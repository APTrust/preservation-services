# APTrust Preservation Services

This is the testing ground for a rewrite of APTrust's Exchange services.

poc.go is a proof of concept for some of the achitectural changes we're
considering.

## Running

You'll need the following in your environment:

```
export AWS_ACCESS_KEY_ID="some key that can access aptrust buckets"
export AWS_SECRET_ACCESS_KEY="a valid secret key"
export GO111MODULE=on
```

The first two are for S3 connections. The last allows Go to access versioned
modules, such as Minio V6.

You also need Go 1.11 or later. Preferably Go 1.13 or later.

With all that, you can run:

`go run poc.go`

## Redis

You'll need redis installed to run redis.go, which doesn't do much yet other than
prove it can connect, save a key and retrieve a key.

On Mac: `brew install redis`

Start: `brew services start redis`

Stop: `brew services stop redis`

Or: `redis-server /usr/local/etc/redis.conf`

Redis runs on localhost:6379. By default, its DB files are in
/usr/local/var/db/redis/.

# Testing

`APT_SERVICES_CONFIG=test go test ./...`

For dev and local test environments, you'll need write permissions in your own
home directory, which you should already have. Specifically, in dev and local
test modes, models/common/config.go will panic if it can't create and write to
these directories:

```
~/tmp/pres-serv/ingest
~/tmp/pres-serv/logs
~/tmp/pres-serv/restore
```

All of those files are considered temporary, so no worries if you delete them.
