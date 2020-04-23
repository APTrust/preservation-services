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

3. Check to see if this is an update of an existing bag
   - If so:
      - Overwrite UUIDs in Redis data with UUIDs of existing files
      - Check fixity of new files vs. existing & mark files that need re-save.
   - We need to port the Pharos Client from Exchange and do the following:
      - Loop through IngestFile objects in Redis.
      - Look up each file in Pharos.
      - If the file exists in Pharos (even if it's been deleted), overwrite
        the IngestObject UUID and re-save to Redis.

4. Unpack to staging bucket
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

5. File Format Identification
   - We'll be using [FIDO](https://github.com/openpreserve/fido) for file
     identification. After reviewing a number of tools including JHOVE,
     DROID, FITS, and Apache Tika, this was by far the simplest to set up,
     configure, and integrate into our workflow. It will also be the
     easiest to maintain.
   - File Format Id has some requirements outside the ingest workflow,
     including a long-standing open reqest to retroactively identify
     files already in preservation storage. We will likely run format
     identification AFTER files have been ingested. (The pre-fetch worker
     in step 1 above will make a preliminary guess about the file type
     based on extension. That guess will stick until we run the format
     identifier worker against the file in preservation storage, unless
     it's Glacier-only or Glacier Deep.)
   - Workflow for this worker when checking items in preservation storage:
       - Find active Generic Files in Pharos that have no file format
         identification. (How to do this? Add a field to GF record?)
       - For each file, get the UUID primary storage URL, and file
         extension (from the GF identifier).
       - Create a signed URL to retrieve to scan the file using the
         [identify_format](./scripts/identify_format.sh) script.
         Use UUID + extension for filename.
       - If possible (and it may not be), change the ContentType of the
         object in S3 and Glacier storage. Do NOT rewrite the object
         because that risks corruption. It may not be possible to update
         ContentType in S3 an especially in Glacier, so we may have to
         skip this step for older items already in preservation storage.
       - Save the file format back to Pharos in the GenericFile record,
         and add a timestamp indicating when the format was identified.
   - Workflow for this worker when checking items in ingest staging:
       - Get the Redis record for each IngestFile.
       - If it's already been identified, skip.
       - Run through [identify_format](./scripts/identify_format.sh) as above.
       - Record the file format and timestamp in the IngestFile struct.
       - Update the ContentType attribute on the file in the staging bucket.
         See this [CopyObject comment](https://github.com/minio/minio/commit/69559aa101d4b3b28b9eeb09db9850e4d56f9aa7#diff-89e76f773faa587ce9a0b1ccec21c649R220) for info on how to do that. It should update the metadata without affecting the content of the object itself. That commit fixes [this bug](https://github.com/minio/minio/issues/3316) which describes our issue.
       - Save the IngestFile back to Redis.

6. Store
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

7. Storage validation
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

8. Record
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

9. Cleanup
   - Delete S3 files from staging
   - Delete tar file from ingest bucket
   - Delete Redis metadate (or copy as JSON to S3)

# Scaling Notes

The shared-nothing architecture of the new preservation-services code was designed
for horizontal scaling. However, when scaling horizontally, we don't need to add
all nine workers to each additional server. Doing so can cause problems, because
all of the workers make heavy use of Redis, and a few make heavy use of Pharos.
These are noted below.

99.9% of ingest bottlenecks come from the time it takes to copy files to S3. (The
other 0.1% comes from the time it takes to record bags with tens of thousands of
files in Pharos. Those bags are rare.)

Because S3 is the source of latency, and because S3 can handle way more requests
than a single instance of Pharos or even Redis, the workers we want to scale are
those that have lots of S3 I/O. We can scale those across several servers
(optimized for bandwidth) while keeping only a single instance of other workers.

It may make sense to run Pharos, Redis, and a handful of non-S3-intenstive workers
on one machine so the workers can talk to Pharos and Redis over the loopback
interface and not use up network bandwidth.

Here's a list of the ingest workers and their estimated resource usage. Workers
with an asterisk are candidates for horizontal scaling onto additional servers
optimized for network I/O. The Storage Worker with two asterisks is a special
case.

1. Pre-Fetch Worker
  - S3 read (high)
  - S3 write (low-moderate)
  - Redis read (high)
  - Redis write (high)
  - Pharos read (low)
  - Pharos write (low)
  - CPU (moderate-high)
  - Memory (low-moderate)

2. Validation Worker
  - S3 read (none)
  - S3 write (none)
  - Redis read (high)
  - Redis write (high)
  - Pharos read (low)
  - Pharos write (low)
  - CPU (moderate-high)
  - Memory (low-moderate)

3. Reingest Check Worker
  - S3 read (none)
  - S3 write (none)
  - Redis read (high)
  - Redis write (high)
  - Pharos read (high)
  - Pharos write (low)
  - CPU (low)
  - Memory (low)

4. Staging Worker *
  - S3 read (high)
  - S3 write (high)
  - Redis read (high)
  - Redis write (high)
  - Pharos read (low)
  - Pharos write (low)
  - CPU (moderate)
  - Memory (low-moderate)

5. File Characterization Worker *
  - S3 read (moderate-high)
  - S3 write (none)
  - Redis read (high)
  - Redis write (high)
  - Pharos read (low)
  - Pharos write (low)
  - CPU (moderate)
  - Memory (moderate)

6. Storage Worker **
  - S3 read (low when copying to AWS, high when copying outside of AWS)
  - S3 write (low when copying to AWS, high when copying outside of AWS)
  - Redis read (high)
  - Redis write (high)
  - Pharos read (low)
  - Pharos write (low)
  - CPU (moderate-high)
  - Memory (low when copying to AWS, high when copying outside of AWS)

7. Storage Validation Worker *
  - S3 read (low)
  - S3 stat (high number of HEAD/Stat calls, not much data)
  - S3 write (none)
  - Redis read (high)
  - Redis write (high)
  - Pharos read (low)
  - Pharos write (low)
  - CPU (low)
  - Memory (low)

8. Record Worker
  - S3 read (none)
  - S3 write (none)
  - Redis read (high)
  - Redis write (high)
  - Pharos read (low)
  - Pharos write (high)
  - CPU (low-moderate)
  - Memory (low)

9. Cleanup Worker
  - S3 read (low)
  - S3 write (none)
  - S3 delete (high)
  - Redis read (high)
  - Redis write (low)
  - Redis delete (high)
  - Pharos read (low)
  - Pharos write (low)
  - CPU (low)
  - Memory (low)

** It may make sense to run StorageWorkers that read from separate NSQ topics.
For example, one StorageWorker can handle all objects to be preserved in the AWS
environment, while another can handle storage in non-AWS environments like
Wasabi. The system will know from an object's StorageOption property where
it should be preserved.

# Redis Persistence

Redis must be configured to persist all data to disk, so that it remains available
after the service restarts.

## Redis as NSQ Replacement?

Redis may be able to replace NSQ as the queue service. (No sense in running both
services if redis can handle all the work. Also, our long-running tasks are not
suited to NSQ's hard-coded timeout limits.)

See [RMQ](https://github.com/adjust/rmq)

## Rails as NSQ Replacement?

Consider also adding an endpoint to the WorkItems controller to get WorkItems
ready for processing. Because NSQ may deliver the same message to multiple
consumers when it thinks a message has timed out, we've always had to
double-check every NSQ message against the WorkItem it refers to, with the
Pharos WorkItem being the authoritative record. If we have to check this item
every time, no matter what, consider checking only the WorkItem and cutting
NSQ from the mix.

Unlike NSQ, Redis/RMQ does guarantee it won't deliver the same message to
multiple consumers, but it does not have a simple requeue feature nor does it
detect timeouts. That means unprocessed messages can fall through the cracks,
necessitating a scan of all WorkItems to requeue those that were lost. That
requeuing would inevitably lead to duplicates in the queue, again
necessitating double-checking every queued item against the Pharos WorkItem
record to ensure it hasn't already been given to another worker.

So again, why not make Pharos the queue and implement a pull model where
workers ask for a batch of items at a time? This could also allow us to
query for specific items so that, for example, one worker could focus on
ingesting large bags while others could work on smaller bags. Each worker
could ask Pharos for WorkItems that pertain to bags matching specific
criteria, such as as size, age, etc.

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

See the stream-to-stream uploading in [PreservationUploader.CopyToExternalStorage](https://github.com/APTrust/preservation-services/blob/master/ingest/preservation_uploader.go#L126). If the stream can go through a tar writer, we could do this:

1. Pass each file from perservation, through multiwriter, to restoration, calculating manifest data along the way.
2. End by writing manifests into the tar stream/restoration staging bucket with name like <WorkItemId>-Restore.tar.
3. Validate the restored bag as we do during ingest with MetadataGatherer and MetadataValidator.
4. Copy file from restoration staging to depositor's restoration bucket using S3's server-to-server copy.

That may be more feasible than the original idea, which is:

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

## TODOs

See todos in util/testutil/mock_services.go and in models/common/config.go
