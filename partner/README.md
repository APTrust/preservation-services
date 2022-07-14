# APTrust Partner Tools

This directory contains a the latest version of the command-line applications that APTrust has been distributing to depositors for several years. While the existing tools (version 2.x and earlier) talk to Pharos, these will talk to registry.

The new tools will not include apt_delete, apt_download, apt_list, or apt_upload. Since those are basic S3 operations, we now refer depositors to the more robust and full-featured [Minio Client](https://docs.min.io/docs/minio-client-quickstart-guide.html). It's well documented, well supported, and runs on Windows, Mac, and Linux.

That leaves the following tools:

* **apt_check_ingest** for checking the status of pending and completed ingests.
* **apt_validate** for validating bags.
