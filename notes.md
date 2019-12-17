# Notes on new fetch/validate process

1. Pre-fetch worker collects stats on bag.
   - Download from receiving bucket through Tar reader.
   - Tar reader collects stats including
       - file name
       - size
       - md5 checksum
       - sha256 checksum
       - uuid (new or from existing file in pharos)
       - pharos md5 (for existing files)
       - pharos sha256 (for existing files)
   - Tar reader stores this data in json format in redis
   - Tar reader parses manifests, updates redis json with
       - manifest md5 (if present)
       - manifest sha256 (if present)
       - tagmanifest md5 (if present)
       - tagmanifest sha256 (if present)
   - Tar reader parses the following tag files and stores as json in redis
       - bagit.txt
       - bag-info.txt
       - aptrust-info.txt
   - OK?
       - Update WorkItem and push to next queue.
   - Transient errors?
       - Update WorkItem and requeue.
   - Fatal errors?
       - Mark WorkItem failed with errors.

2. Validation worker validates bag using data from redis
   - Has required tag files.
   - Has required tags with valid values.
   - All files present.
   - All checksums match.
   - No extraneous files in payload directory.
   - OK?
       - Update WorkItem and push to next queue
   - Transient errors?
       - Update WorkItem and requeue
   - Fatal errors?
       - Mark WorkItem failed with error messages
       - Delete data from redis

3. Unpack to staging bucket
   - Download through tar reader, saving each file individually to staging bucket.
   - Update redis JSON record for each successfully unpacked file to show file
     was copied to staging bucket.
   - OK?
       - Update WorkItem and push to next queue.
   - Transient errors?
       - Update WorkItem and requeue
   - Fatal errors?
       - Mark WorkItem failed with error messages
       - Delete data from redis

4. Store
   - Copy each file from staging bucket to preservation and/or glacier, using
     minio's CopyObject method.
   - Update each file's JSON record in redis
       - Add preservation urls to each record
       - Timestamp when each S3/Glacier copy was made
   - OK?
       - Push to next queue
   - Transient errors?
       - Retry internally several times, then
       - Update WorkItem and requeue
   - Fatal errors?
       - Mark WorkItem failed with error messages
       - Delete data from redis

5. Storage validation
   - Validate that each generic file exists at each of its preservation URLs
       - etag
       - size
       - metadata
   - Update each redis file record with validation info.
   - OK?
       - Push to next queue
   - Transient errors?
       - Retry internally several times, then
       - Update WorkItem and requeue
   - Fatal errors?
       - Mark WorkItem failed with error messages
       - Delete data from redis

6. Record
   - Pull object record from redis, transform and copy to Pharos.
   - Pull file records from redis, transform and copy to Pharos.
   - OK?
       - Push to next queue
   - Transient errors?
       - Retry internally several times, then
       - Update WorkItem and requeue
   - Fatal errors?
       - Mark WorkItem failed with error messages
       - Delete data from redis

# Redis Persistence

Redis must be configured to persist all data to disk, so that it remains available
after the service restarts.

## Redis as NSQ Replacement?

Redis may be able to replace NSQ as the queue service. (No sense in running both
services if redis can handle all the work. Also, our long-running tasks are not
suited to NSQ's hard-coded timeout limits.)

# JSON Data

Each WorkItem will have a redis entry for the following:

- IntellectualObject
- GenericFiles (one per file)
- ProcessingState

## Intellectual Object

Key: `WorkItemId:ObjectIdentifier`

Example: `31337:virginia.edu/libra-20191231`

Value: ObjectJSON (to be determined, but will be similar to IntellectualObject in Exchange)

## Generic File

Key: `WorkItemId:GenericFileIdentifier`

Example: `31337:virginia.edu/libra-20191231/data/images/photo1.jpg`

Value: ObjectJSON (to be determined, but will be similar to GenericFile in Exchange)

## Processing State

Key: `WorkItemId:State`

Example: `31337:State`

Value: ObjectJSON (similar to the operation results collection in WorkItemState)

## Storage and Fetching

To get all keys related to a WorkItem, use [scan](https://redis.io/commands/scan) or store items as hashes using [hset](https://redis.io/commands/hset) and [hget](https://redis.io/commands/hget). Use (hscan)[https://redis.io/commands/hscan] and/or (hkeys)[https://redis.io/commands/hkeys] to list keys.

# Fixity and Deletion

The fixity and deletion workers would not be affected by the architectural changes described above.

# Notes on Restoration changes

Will we still need a staging disk for restoration? Or use minio's [ComposeObject](https://docs.min.io/docs/golang-client-api-reference.html#ComposeObject)? Or can we stream on the fly to an S3 restoration bucket?

Problems:

1. ComposeObject just writes everything into one big blob.
2. If we encounter a single problem in on-the-fly tar writing, we'll likely have to rewrite the entire tar file from scratch. That's both likely and a problem with vary large bags.
3. How can we validate the tar file if it was written to an S3 bucket? Easier to validate locally. (Though we could employ the pre-fetch and validation services described above.)

It's possible to mount a temporary disk to handle restorations. Full bag restorations are rare, and we know the approximate size of the bag before we restore it. We could do something like this:

1. Mount a new temporary drive 1.1 times the size of the bag.
2. Write the tar file to disk on the temporary mount.
3. Validate the tar file.
4. Copy the tar file to the restoration bucket.
5. Delete the tar file.
6. Unmount the temp drive.

This could allow restoration to scale horizontally, and could be less painful than autoscaling logical volumes, which requires temporary service interruptions.

This would present a problem in the theoretical case of mass restoration. If we're restoring dozens of bags at once, would we have trouble mounting and unmounting dozens of drives?