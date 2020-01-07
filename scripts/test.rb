# Run unit and integration tests for preservation-services.
# First we need to start redis and minio servers.
#
# Minio user:     minioadmin
# Minio password: minioadmin
# Run with: minio  server --quiet --address=127.0.0.1:9899 ~/tmp/minio
#
# Need to make the following buckets:
# const ReceivingBucket = "receiving"
# const StagingBucket = "staging"
# const PreservationBucket = "preservation"
# const ReplicationBucket = "replication"
#
# This script should ensure ~/tmp/minio exists.
# It may be able to create the buckets listed above as well.
#
# Need to start redis server with in-memory storage.
