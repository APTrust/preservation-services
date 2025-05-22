# Reingest Test Bags

This folder, and the directories inside it, contain three versions of the same bag. The first version, in 01-Standard, uses the Standard storage option. The second, in 02-Glacier-OH, uses Glacier-Ohio. The third, in 03-Wasabi-VA, uses the Wasabi-Virginia storage option.

These bags are used to test the fixes for storage mismatch re-ingest bug documented in https://trello.com/c/iypSuBvB and https://trello.com/c/C4XlgSNU.

In that bug, if a bag was originally ingested with storage option X and later re-ingested with storage option Y, the system would wind up with copies of the bag's files in both X and Y. The proper behavior is to have all files in option X only, since our system has a rule saying that whatever storage option you chose for the intial ingest will apply to all subsequent ingests. (We do this to avoid having different versions of files in different preservation buckets.)

Our test ingests the first (Standard) version of the bag, then reingests the other two. The test then verifies that after all three ingests are complete, the preserved files exist in standard storage only, and not in Glacier or Wasabi.
