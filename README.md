# APTrust Preservation Services

This is the beginning of a rewrite of APTrust's Exchange services. The goals
are:

* Simpler, more modular code
* Full test coverage
* Use S3 insteady of dynamically resized EBS for staging,
  to simplify environment and reduce costs
* Replace EBS and BoltDB to enable horizontal scalability
* Replace the AWS SDK with the simpler Minio library
* Support Beyond the Repository BagIt format

All of these are easier to implement in a rewrite than to work into the old
Exchange codebase, which has accumulated quite a bit of cruft over the years.

To run, you'll need the following in your environment:

```
export AWS_ACCESS_KEY_ID="some key that can access aptrust buckets"
export AWS_SECRET_ACCESS_KEY="a valid secret key"
export GO111MODULE=on
```

# Testing

To run unit tests: `ruby scripts/test.rb units`

To run integration tests: `ruby scripts/test.rb integration`

Note that integration tests require the following:

1. Docker
2. A local installation of the [Pharos source](https://github.com/APTrust/pharos)
3. An environment variable called `PHAROS_ROOT` set to the absolute path of your Pharos source code installation.

The unit tests start two lightweight services, redis and minio, Mac and Linux versions of which are included in this source repo.

The integration tests start all of the services that unit tests start, plus nsqd, nsqlookupd, nsqdadmin (all provided in this repo) and a Docker instance of Pharos.
