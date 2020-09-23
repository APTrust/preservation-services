// +build e2e

package e2e_test

type ExpectedObject struct {
	TarFileName      string
	ObjectIdentifier string
	StorageOption    string
	InDirectory      string // original or updated
	Files            []TestFile
}

type TestFile struct {
	Identifier string
	FileFormat string
	Size       int
	Sha256     string
}

/*
---- ORIGINAL BAGS ----
    test.edu.apt-001.tar
    test.edu.apt-002.tar
    test.edu.btr-001.tar
    test.edu.btr-002.tar
    test.edu.glacier-deep-oh.tar
    test.edu.glacier-deep-or.tar
    test.edu.glacier-deep-va.tar
    test.edu.glacier-oh.tar
    test.edu.glacier-or.tar
    test.edu.glacier-va.tar
    test.edu.standard-storage.tar
    test.edu.wasabi-or.tar
    test.edu.wasabi-va.tar

---- UPDATED BAGS ----
    test.edu.apt-001.tar
    test.edu.apt-002.tar
    test.edu.btr-001.tar
    test.edu.btr-002.tar

*/
