# Integration Test Bags

This directory contains bags for integration testing. All bags are valid and
should ingest without error. The "original" folder contains version 1 of each
bag, while the "updated" folder contains updated versions of each. The
updated bags contain mostly the same files, with these differences:

* data/files/movie.avi -> deleted
* data/files/image.jpg -> deleted
* data/files/data.csv  -> changed
* data/files/data.json -> changed
* data/files/data.xml  -> changed
* data/files/.DS_Store -> added
* data/files/file_example_SVG_20kB.svg -> added

The "restoration" folder contains bags for restoration tests in
restoration/bag\_restorer\_int\_test.go. It also contains files to be restored
in the files directory. During integration tests, those files, whose names
match the UUIDs in Pharos fixture data, are copied into local Minio
preservation buckets by bag\_restorer\_int\_test.go so the restoration tests
can proceed.
