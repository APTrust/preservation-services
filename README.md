# APTrust Preservation Services

This is the completed rewrite of APTrust's Exchange services and related infrasrtucture. The goals
are:

* Simpler, more modular code
* Full test coverage
* Use S3 insteady of dynamically resized EBS for staging,
  to simplify environment and reduce costs
* Replace EBS and BoltDB to enable horizontal scalability
* Replace the AWS SDK with the simpler Minio library
* Support Beyond the Repository BagIt format
* More simple, agile, and resilient infrastructure, using Infrasrtucture as Code.

All of these are easier to implement in a rewrite than to work into the old
Exchange codebase, which has accumulated quite a bit of cruft over the years.

To run, you'll need the following in your environment:

```
export AWS_ACCESS_KEY_ID="some key that can access aptrust buckets"
export AWS_SECRET_ACCESS_KEY="a valid secret key"
export GO111MODULE=on

# APT_CONFIG_DIR tells the executables where to look for the config file.
# If not set, it defaults to the current working directory.
export APT_CONFIG_DIR=/path/to/config/dir

# APT_ENV tells the apps which config file to load. The config file name
# follows the pattern .env.<config_name>. E.g. .env.test, .env.demo, etc.
export APT_ENV=test
```

# Testing

TLDR: Whenever you make changes to preservation services code, and whenever you make changes to Registry code that might affect Registry's behavoir, you should run:

`./scripts/test.rb integration && ./scripts/test.rb e2e`

That runs Preserve's integration tests and end-to-end (e2e) tests to ensure that Preserve and Registry still work together as expected. The end-to-end test suite contains the tests most likely to catch subtle regressions. This suite runs a series of ingests, restorations and deletions and then carefully checks for all expected post-conditions, such as alerts and premis events created, presence of specific files in specific S3 buckets, etc.

The integration tests usually take 1-2 minutes to complete. The end-to-end (e2e) tests can take 5-10 minutes, depending on your machine.

**Tests require Docker to be installed and running.**

## Unit Tests

To run unit tests: `ruby scripts/test.rb units`

The unit tests start two lightweight services, redis and minio, Mac and Linux versions of which are included in this source repo.

## Integration Tests

To run integration tests: `ruby scripts/test.rb integration`

Note that integration tests require the following:

1. A local installation of the [Pharos source](https://github.com/APTrust/pharos)
2. An environment variable called `PHAROS_ROOT` set to the absolute path of your Pharos source code installation.

The integration tests start all of the services that unit tests start, plus nsqd, nsqlookupd, nsqdadmin (all provided in this repo) and a Docker instance of Pharos.

Note: Integration test files end with `_int_test.go` and include the following build tag on the first line:

```
// +build integration

```

## Testing Registry Client Only

First, run the registry with freshly loaded integration fixtures from the root of the registry project:

```
APT_ENV=integration ./registry serve
```

Then run only the Registry client tests in Preservation Services:

```
APT_ENV=test go test -tags=integration network/registry_*_test.go
```

Note that you should restart the Registry service with the first command above each time you want to re-run the client tests. This ensures that the Registry always starts with the same set of known fixtures.

## End to End Tests

To run integration tests: `ruby scripts/test.rb e2e`

End to end tests have the same system requirements as integration tests, starting
instances of Redis, NSQ, Minio, and Pharos. The use the `+build e2e` build tag.

These tests upload a number of bags to the local Minio receiving bucket and let
the services work from there as the would in production. The bucket reader finds
the new bags, creates WorkItems, and queues them in NSQ.

When items complete ingest, they go into NSQ topic e2e\_ingest\_post\_test. When
all items have been ingested, updated versions of some bags go into the
receiving bucket for reingest.

After reingest, the tests initiate some file and object restorations, and some
fixity checks.

After each major step (ingest + reingest, restoration, and fixity checking), the
tests ensure that all expected records (objects, files, checksums, storage
records, premis events, and work items) exist in Pharos and that all files are
in the right places in S3/Glacier/Wasabi. They also test that temporary records
are cleaned out of Redis, and that temp files are removed from the staging
bucket.

These tests take 5-10 minutes run and are not meant to be run with every commit.
They should be run after code refactoring, the addition of new features, and the
updating of underlying libaries, as a sanity check __after__ all integration
tests have passed.

They do not test file and object deletion because those actions require
multi-step email confirmation workflows that we can't easily simulate.

Integration tests do cover file and object deletion, and you can test those
actions manually using interactive testing.

# Interactive Testing

You can launch interactive tests with `./scripts/test.rb interactive`

With interactive tests, you can bag items with DART and push them through the
ingest, re-ingest, restoration and deletion processes. This can be useful for
load testing, testing bags that cause obscure bugs, and getting a feel for the
general user experience.

If you're going to tweak Pharos code during interactive tests, be sure to set `config.cache_classes = false` in the Pharos file
`config/envionments/integration.rb`.

We do not use docker in local integration or interactive tests because it is
abysmally slow on Mac OS, particularly when dynamically reloading Rails code.

## Settings for Interactive Tests

Pharos will be running at `http://localhost:9292` with login `system@aprust.org`
and password `password`.

Redis will be running on `localhost:6379`

The NSQ admin panel will run at `http://localhost:4171`

The Minio control panel will run at `http://localhost:9899` with login
`minioadmin` and password `minioadmin`.

If you want to push bags from DART into this locally running system, you'll need
to follow the [settings import instructions](https://aptrust.github.io/dart-docs/users/settings/import/),
cutting and pasting the JSON below.

```json
{
  "id": "00000000-0000-0000-0000-000000000000",
  "appSettings": [],
  "bagItProfiles": [],
  "questions": [],
  "remoteRepositories": [],
  "storageServices": [
    {
      "id": "fd40a9a1-8301-45cf-9550-1d8ed6d996a0",
      "name": "Local Minio S3 Service",
      "description": "Minio server on localhost",
      "protocol": "s3",
      "host": "localhost",
      "port": 9899,
      "bucket": "aptrust.receiving.test.test.edu",
      "login": "minioadmin",
      "password": "minioadmin",
      "loginExtra": "",
      "allowsUpload": true,
      "allowsDownload": true
    }
  ]
}
```

# Docker Build & Deploy

On our staging, demo, and production systems, we wrap all services in Docker
containers. You can build the containers locally with `make release`, or wait
for Travis to build them after you push a commit to GitHub.

Travis build the containers after each push, if the tests pass. The test +
build process usually takes about 20 minutes.

Regardless of where the container build is initiated, deploy with ansible:

`ansible-playbook preserv.staging.docker.yml --diff`

# Deployment Notes and Infrastructure

The infrastructure for the new preservation-services has been updated to operate on the AWS PaaS offering Elasatic Container Service (ECS).
The containers were updated to operate in a loosely coupled manner, providing greater flexibility for environments.

All environments, staging, demo, and production, are running on ECS Fargate, with each of the environments built using AWS' CloudFormation, with Ansible for configuration management. Unlike the legacy environment, this new infrastructure is completely built from Infrastructure As Code.

Logging is now set to be passed to Cloudwatch, the AWS aggregated logging service, with performance monitoring.

Worker logs are now directed to STDOUT by setting the following in the .env file OR in the AWS Parameter Store:

```
LOG_DIR="STDOUT"
```

Persistent storage for NSQ is provided using AWS' Elastic File Service (EFS). Redis is provided using the AWS PaaS offering elasticache, removing the need to maintain Redis containers, or scale them.

Logging that occurs in the test scripts is handled differently according the scripts, and has no impact on the above.

Access to the containers directly using the docker exec functionlality is possible in this environment, if you have been given the permissions to actually do so. Please review this blog post.

https://aws.amazon.com/blogs/containers/new-using-amazon-ecs-exec-access-your-containers-fargate-ec2/
