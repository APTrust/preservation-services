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

# APT_CONFIG_DIR tells the executables where to look for the config file.
# If not set, it defaults to the current working directory.
export APT_CONFIG_DIR=/path/to/config/dir

# APT_ENV tells the apps which config file to load. The config file name
# follows the pattern .env.<config_name>. E.g. .env.test, .env.demo, etc.
export APT_ENV=test
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

Note: Integration test files end with `_int_test.go` and include the following build tag on the first line:

```
// +build integration

```

# Interactive Testing

You can launch interactive tests with `./scripts/test.rb interactive`

If you're going to tweak Pharos code during interactive tests, be sure to set `config.cache_classes = true` in the Pharos file `config/envionments/docker_integration.rb`.

# Docker Build & Deploy

Wait for Travis to build the docker containers, or build locally with `make release`.

Regardless of where the container build is initiated, deploy with ansible:

`ansible-playbook preserv.staging.docker.yml --diff`

# Deployment Notes

Logs, NSQ, temp files and Redis aof files are in `/data/preserv`.

Source tree is in `/srv/docker/preserv`

To see stdout and stderr of workers:

```
cd /srv/docker/preserv
sudo docker-compose logs -f ingest_staging_uploader
```

...or `sudo docker-compose logs -f` to tail all services

# Staging Notes

To run the Redis CLI on staging:

`docker run -it --rm redis redis-cli --version`

## Running One-Off Fixity Checks on Staging

To run one-off fixity checks on specific files on staging:

Get the container id of the fixity queue worker with
`sudo docker ps | grep apt_queue_fixity`

Then, using that container id, run `sudo docker exec -it <container id> /bin/sh`
to get a brain-dead shell.

Since the executable is in the container's root directory, the following command
will queue your file: `./apt_queue_fixity <generic file identifier>`

Or you can do all this in one line with:

`sudo docker exec -it <container id> /bin/sh -c "./apt_queue_fixity '<generic file identifier>'"`
