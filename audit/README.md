# Auditing Files in Preservation Storage

The auditor in this directory checks files in preservation storage to ensure
they look correct. Use this as a sanity check when necessary.

When run with the `-f=false` option, it will issue only HEAD reqeusts to get
object metadata from S3, Glacier, and Wasabi.

The auditor does the following checks for each file:

* Compares the Registry's file size to the size of the file in preservation
  storage
* Compares Registry's md5 checksum for the file against the file's e-tag in
  preservation storage, when possible. This is not possible for e-tags
  containing a dash. Those are not proper md5 digests.
* Compares all of the object's metadata headers in preservation storage
  against the metadata in Registry.

If any of the above comparisons fail, the file is marked as failed. You
can run a checksum on failed file by running the auditor again using with
the `-f=true` option. (`f` means to run a full sha256 checksum.)

If you do run an entire batch with option `-f=true`, note that the auditor
will run checksums only on files that fail one or more of the above tests.

## Building the Auditor

Change into the audit directory of the preservation services project and
run `go build`. This will compile `main.go` into a binary called `audit`
within the audit directory.

## Input

To run the auditor, you'll need a file containing a list of GenericFile IDs
with one ID per line. Leading and trailing whitespace around the ids will
be ignored.

Your input file should look something like this:

```
4382080
4382168
4382878
4388254
4388359
4389469
4391599
4392515
4393958
```

These are the ids of the files you want to check. You can get a list by
running a query on the production bastion host, like the one below, which
collects the ids of all files ingested or re-ingested between Nov. 1 and
Nov. 12, 2022.

First, ssh into the production bastion host. Then run `./db_connect`. That
will give you a psql prompt.

```sql

db => \o file_ids.csv

db => select distinct(generic_file_id)
      from premis_events
      where event_type='ingestion'
      and generic_file_id is not null
      and created_at > '2022-11-01'
      and created_at < '2022-11-12'
      order by generic_file_id;

```

The first statement above, `\o file_ids.csv`, tells psql to write the
results of the query to a file called `file_ids.csv` instead of displaying
them onscreen.

The query itself pulls the Generic File IDs of all files ingested between
the specified dates. Note that querying the `generic_files` table for all
file IDs where `updated_at` falls within our date range will not work
because the `generic_files.updated_at` timestamp is updated every time we
run a new fixity check on a file.

If you do want to query the `generic_files` table, for instance, to check
all files belonging to a specific depositor, be sure to include the filter
`where state='A'` in your query. Otherwise, you'll get IDs of deleted files
and those are guaranteed to fail a check, because they no longer exist.

## Running the Auditor

Once you have a file containing a list of generic file ids, you can run
the auditor by changing into the preservation services root directory
(one up from the audit directory) and running something like this:

```

APT_ENV=audit ./audit/audit -i audit/file_ids.csv -f=false > audit_output.csv 2>quick_results.txt

```

Breaking this down:

* **APT_ENV=audit** tells the auditor to use the config file .env.audit,
  which contains most of the basic config settings to audit files in
  the production repo.
* **./audit/audit** invokes the auditor you just compiled
* **-i audit/file_ids.csv** specifies the input file containing your list
  of generic file ids.
* **-f=false** tells the auditor not to do full fixity checks on files
  that fail a basic sanity check. Change this to `-f=true` if you do want
  to run full fixity checks on those files, but note that the auditor
  will not check fixity on Glacier files, because it takes hours to
  retrieve them.
* **> audit_output.csv** pipes the detailed output from STDOUT to a file
  called audit_output.csv
* **2>quick_results.txt** pipes the quick output and all error messages
  from STDERR to a file called quick_results.txt

If you just want to dump all output to the console, you can omit the pipes
at the end of the command, and just run this:

```

APT_ENV=audit ./audit/audit -i audit/file_ids.csv -f=false

```

## Exit Codes

* 0 - The auditor completed its work without serious errors, though you should
      check the detailed output to see if it could not check some individual
      files.
* 1 - The auditor didn't run because something was wrong with the command-line
      options.
* 2 - The auditor tried to run but had to exit without completing its work
      due to a fatal error.

## Things to Note

1. The auditor will not check fixity on Glacier files, because it takes
   hours to retrieve them.
2. The `.env.audit` file omits sensitive credentials. These must be available
   as environment variables.
3. The auditor may complain that the log directory, `~/aptrust/audit/logs`,
   does not exist. You may have to create that manually.

You must set the following environment variables for the auditor to have
the credentials required to query the Registry and S3, Glacier, and Wasabi:

* PRESERV_REGISTRY_API_KEY - The API key used to access the Registry.
* PRESERV_REGISTRY_API_USER - The API user (email address) for accessing
  registry.
* S3_AWS_KEY - The AWS Access Key ID used to access Amazon S3 and Glacier.
* S3_AWS_SECRET - The AWS Secret Key used to access Amazon S3 and Glacier.
* S3_NEWSTORAGEOPTION_KEY - The Access Key ID used to access Wasabi.
* S3_NEWSTORAGEOPTION_SECRET - The Secret Key used to access Wasabi.

## Output

The auditor produces two output streams: detailed results go to STDOUT,
which quick results and error messages go to STDERR.

The detailed results are printed in CSV format, so you can open the output
as a spreadsheet, or import it into SQLite for querying.

The quick results can be handy if you want to know if any files at all
failed the check.

The detailed results look like this:

| GenericFileID | CheckPassed | Method | ReasonForCheck | RegistrySize | S3Size | IsGlacierOnlyFile | NeedsGlacierFixityCheck | S3Etag | RegistryMd5 | S3MetaMd5 | RegistrySha256 | S3MetaSha256 | StreamSha256 | MismatchedMetaInstitution | MismatchedMetaBagName | MismatchedMetaPath | MismatchedMetaMd5 | MismatchedMetaSha256 | GenericFileCreatedAt | GenericFileUpdatedAt | S3MetaPathInBag | S3MetaBagName | S3MetaInstitution | PreservationUrl | CheckStartedAt | CheckCompletedAt | GenericFileIdentifier | Error
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
4382080 | true | Quick Match |  | 290 | 290 | false | false | 9823643f3d5890971758cd7a2a7e1840 | 9823643f3d5890971758cd7a2a7e1840 | 9823643f3d5890971758cd7a2a7e1840 | 268e7f1ae553c1a5015f6be332807533fd3d2f40b70e2e596a6dbce799dae517 | 268e7f1ae553c1a5015f6be332807533fd3d2f40b70e2e596a6dbce799dae517 |  | false | false | false | false | false | 2019-07-18T22:00:51Z | 2022-11-09T19:38:52Z | bag-info.txt | fulcrum.org/fulcrum.org.heb-765371884 | fulcrum.org | [redacted url] | 2022-11-11T12:43:19-05:00 | 2022-11-11T12:43:19-05:00 | fulcrum.org/fulcrum.org.heb-765371884/bag-info.txt |
4382168 | true | Quick Match |  | 290 | 290 | false | false | 126a60f1253a6d5070c153f86ce5529a | 126a60f1253a6d5070c153f86ce5529a | 126a60f1253a6d5070c153f86ce5529a | 9255d856f374a8218d0e4a0c593f3649347659a0b34861abf5561eed158c0441 | 9255d856f374a8218d0e4a0c593f3649347659a0b34861abf5561eed158c0441 |  | false | false | false | false | false | 2019-07-18T22:01:45Z | 2022-11-09T14:38:04Z | bag-info.txt | fulcrum.org/fulcrum.org.heb-m900nt76t | fulcrum.org | [redacted url] | 2022-11-11T12:43:19-05:00 | 2022-11-11T12:43:20-05:00 | fulcrum.org/fulcrum.org.heb-m900nt76t/bag-info.txt |

The quick results look like this:

```
4382080 passed
4382168 passed
4382878 passed
4388254 passed
4388359 passed
4389469 passed
4391599 passed
4392515 passed
4393958 passed
```

If you're outputting quick results to a file called quick_results.txt, you
can get a rough count of how many checks have completed by running
`wc -l quick_results.txt`.

Running `grep -i failed quick_results.txt` will show you which files, if
any, failed the check.

## Output Fields

The detailed output file contains the following fields:

| Field | Description |
| ----- | ----------- |
GenericFileID | The id of the generic file.
CheckPassed | True or false, indicating whether the check passed.
Method | The method used to check the file. Quick Check uses a size and possibly e-tag comparison. Full Check runs a sha256 checksum.
ReasonForCheck | Indicates why the auditor thought this file needed a full checksum. This will be blank if the Quick Check matches.
RegistrySize | The size of this file, according to the Registry.
S3Size | The size of this file, according to S3, Glacier or Wasabi.
IsGlacierOnlyFile | True or false to indicate wither this file lives in Glacier only.
NeedsGlacierFixityCheck | True or false indicating whether this Glacier-only file needs a fixity check. If it needs one, you'll have to do that on your own.
S3Etag | This file's etag in preseration storage.
RegistryMd5 | The registry's md5 checksum for this file. This should match S3Etag, if the etag contains no dashes.
S3MetaMd5 | The value of our custom Md5 header on this file. It should match the Registry's md5 checksum.
RegistrySha256 | The registry's sha256 checksum for this file.
S3MetaSha256 | The value of our custom Md5 header on this file. It should match the Registry's md5 checksum.
StreamSha256 | The sha256 checksum that the auditor calculated by streaming the file down from S3. This will be blank if CheckPassed=true and if you ran the auditor with the flag `-f=false`
MismatchedMetaInstitution | Indicates whether there was a mismatch between who Registry thinks owns this file and which institution is stored in the file's custom metadata headers.
MismatchedMetaBagName | Indicates whether there was a mismatch between what Registry thinks this file's object identifier is and what's stored in the file's custom metadata headers.
MismatchedMetaPath | Indicates whether there was a mismatch between what Registry thinks this file's original path in the bag was and what's stored in the file's custom metadata headers.
MismatchedMetaMd5 | Indicates whether there was a mismatch between what Registry thinks this file's md5 digest is and what's stored in the file's custom metadata headers.
MismatchedMetaSha256 | Indicates whether there was a mismatch between what Registry thinks this file's sha256 digest is and what's stored in the file's custom metadata headers.
GenericFileCreatedAt | The date on which this file was created in Registry. This can help if your searching for patterns in files that fail a sanity check. E.g. Maybe they were all first ingested on the same day.
GenericFileUpdatedAt | The date on which this file was updated in Registry. This can help if your searching for patterns in files that fail a sanity check. E.g. Maybe they were all reingested or otherwise manipulated on the same day.
S3MetaPathInBag | The value of our custom bagpath header on this file. This is the path of the file in the original bag.
S3MetaBagName | The value of our custom bagname header on this file. This should match the Registry's IntellectualObjectID.
S3MetaInstitution | The value of our custom institution header on this file. This should match the Registry's institution identifier for this file.
PreservationUrl | The URL of the file in preservation storage. This is the URL of the copy that the auditor checked. There may be replicated copies in Glacier as well, but the auditor checks only the primary copy.
CheckStartedAt | Time at which the audit started for this file.
CheckCompletedAt | Time at which the audit completed for this file. This may be empty if an error prevented the audit from finishing.
GenericFileIdentifier | The file's identifier in Registry.
Error | An error message describing why the audit of this file could not be completed. This should be empty for all successful audits.

# Audit History

## Nov. 11, 2022

The reason this auditor was written!

A flood of ingests from Fulcrum exposed a race condition in the metadata
gatherer, which was not properly isolating temporary copies of tag files.
That could potentially lead to cases where tag files from bag A got copied
to preservation storage as a part of bag B.

The bug would have affected only the bag-info.txt and aptrust-info.txt
files in a number of bags ingested betwen Nov. 1 and Nov. 11, 2022.

To be safe, we ran the auditor on all newly ingested and re-ingested files
from that time period, a total of 21,874 files. All passed.
