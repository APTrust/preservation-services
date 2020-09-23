# End to End Tests

End to end tests push a number of bags through the system and then verify expected
outcomes in Pharos, S3, and Redis. The tests include ingest, re-ingest, fixity
checking and restoration. (Deletions are not currently included because they require
email confirmations. Integration and manual tests cover deletions.)

## Ingest

Ingest tests should prove that we can ingest at least one BTR bag, plus APTrust
bags with the following storage options:

* Standard
* Glacier-OH
* Glacier-OR
* Glacier-VA
* Glacier-Deep-OH
* Glacier-Deep-OR
* Glacier-Deep-VA
* Wasabi-OR
* Wasabi-VA

After each ingest, we should ensure the following in Pharos:

* Object record was created
* Generic file records were created
* Checksums were created
* PREMIS events were created
* Work item was marked complete

We should ensure the following in Redis:

* All interim processing data from each ingest was deleted

We should ensure the following in S3:

* All generic files exist in the correct preservation buckets
* All generic files have expected metadata
* No interim files remain in the staging bucket
* Original bags have been deleted from the receiving bucket

## Reingest

Tests should re-ingest two bags and test everything listed under the Ingest
section above, ensuring only new and changed files were re-ingested.

## Fixity Check

The fixity tests should test one file from each storage option. The post test
should ensure that the expected PREMIS events were recorded in Pharos.

Fixity tests don't need to check S3, since fixity checks don't change anything
in S3. They don't need to check Redis, since fixity checks don't use Redis.

## Restoration Tests

Restoration tests should restore:

* One BTR bag
* One file from each storage option
* One complete bag from each storage option

Tests should ensure the following in Pharos:

* Each work item is marked complete and successful

Tests should ensure the following in S3:

* Restored files are in the proper restoration bucket and match the URL in the
  completed WorkItem
* Restored bag are in the proper restoration bucket and match the URL in the
  completed WorkItem
* Restored bags are valid

Restoration tests don't need to check Redis, since restoration workers don't
use Redis.

## Test Files

All files in the e2e directory should have the build tag `// +build e2e`

### e2e_test.go

* Upload all ingest test bags to receiving bucket.
* Start the following workers:
    * ingest_posttest.go
    * reingest_posttest.go
    * fixity_posttest.go
    * restoration_posttest.go
* When ingest topic reaches expected finished count, e2e should upload
  bags for re-ingest.
* When reingest topic reaches expected finished count, e2e should:
    * queue one file from each bag for fixity check
    * queue one file from each bag for restoration
* After files have been restored, e2e shoud queue eac of the test bags
  for restoration.
* e2e will wait for workers to signal they are done (via NSQ)
* e2e will print test results

### Workers

Worker tests should not use require, since a failure there can shut down the
worker and prevent it from performing further tests.

Each of these workers:

* ingest_posttest.go
* reingest_posttest.go
* fixity_posttest.go
* restoration_posttest.go

Should do the following:

* Test expected outcomes for each object and file
* Tell e2e it's done by sending a message to an NSQ topic

### Test Structures

For each object and file, there should be one test struct containing expected
info about the object and its files. These structs should be available to all
of the test workers.
