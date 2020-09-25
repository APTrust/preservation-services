// +build e2e

package e2e_test

import (
	"path"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/util/testutil"
)

type TestBag struct {
	PathToBag        string
	ObjectIdentifier string
	StorageOption    string
	IsValidBag       bool
	IsUpdate         bool
	Files            []TestFile
}

func (tb *TestBag) TarFileName() string {
	return path.Base(tb.PathToBag)
}

type TestFile struct {
	Identifier  string
	FileFormat  string
	Size        int
	Md5         string
	Sha1        string
	Sha256      string
	StorageURLs []string
}

var TestBags = []*TestBag{
	&TestBag{
		PathToBag:        path.Join(testutil.PathToIntTestBags(), "original", "test.edu.apt-001.tar"),
		ObjectIdentifier: "test.edu/apt-001",
		StorageOption:    constants.StorageStandard,
		IsValidBag:       true,
		IsUpdate:         false,
		Files:            []*TestFile{},
	},
	&TestBag{
		PathToBag:        path.Join(testutil.PathToIntTestBags(), "original", "test.edu.apt-002.tar"),
		ObjectIdentifier: "test.edu/apt-002",
		StorageOption:    constants.StorageStandard,
		IsValidBag:       true,
		IsUpdate:         false,
		Files:            []*TestFile{},
	},
	&TestBag{
		PathToBag:        path.Join(testutil.PathToIntTestBags(), "original", "test.edu.btr-001.tar"),
		ObjectIdentifier: "test.edu/btr-001",
		StorageOption:    constants.StorageStandard,
		IsValidBag:       true,
		IsUpdate:         false,
		Files:            []*TestFile{},
	},
	&TestBag{
		PathToBag:        path.Join(testutil.PathToIntTestBags(), "original", "test.edu.btr-002.tar"),
		ObjectIdentifier: "test.edu/btr-002",
		StorageOption:    constants.StorageStandard,
		IsValidBag:       true,
		IsUpdate:         false,
		Files:            []*TestFile{},
	},
	&TestBag{
		PathToBag:        path.Join(testutil.PathToIntTestBags(), "original", "test.edu.glacier-deep-oh.tar"),
		ObjectIdentifier: "test.edu/glacier-deep-oh",
		StorageOption:    constants.StorageGlacierDeepOH,
		IsValidBag:       true,
		IsUpdate:         false,
		Files:            []*TestFile{},
	},
	&TestBag{
		PathToBag:        path.Join(testutil.PathToIntTestBags(), "original", "test.edu.glacier-deep-or.tar"),
		ObjectIdentifier: "test.edu/glacier-deep-or",
		StorageOption:    constants.StorageGlacierDeepOR,
		IsValidBag:       true,
		IsUpdate:         false,
		Files:            []*TestFile{},
	},
	&TestBag{
		PathToBag:        path.Join(testutil.PathToIntTestBags(), "original", "test.edu.glacier-deep-va.tar"),
		ObjectIdentifier: "test.edu/glacier-deep-va",
		StorageOption:    constants.StorageGlacierDeepVA,
		IsValidBag:       true,
		IsUpdate:         false,
		Files:            []*TestFile{},
	},
	&TestBag{
		PathToBag:        path.Join(testutil.PathToIntTestBags(), "original", "test.edu.glacier-oh.tar"),
		ObjectIdentifier: "test.edu/glacier-oh",
		StorageOption:    constants.StorageGlacierOH,
		IsValidBag:       true,
		IsUpdate:         false,
		Files:            []*TestFile{},
	},
	&TestBag{
		PathToBag:        path.Join(testutil.PathToIntTestBags(), "original", "test.edu.glacier-or.tar"),
		ObjectIdentifier: "test.edu/glacier-or",
		StorageOption:    constants.StorageGlacierOR,
		IsValidBag:       true,
		IsUpdate:         false,
		Files:            []*TestFile{},
	},
	&TestBag{
		PathToBag:        path.Join(testutil.PathToIntTestBags(), "original", "test.edu.glacier-va.tar"),
		ObjectIdentifier: "test.edu/glacier-va",
		StorageOption:    constants.StorageGlacierVA,
		IsValidBag:       true,
		IsUpdate:         false,
		Files:            []*TestFile{},
	},
	&TestBag{
		PathToBag:        path.Join(testutil.PathToIntTestBags(), "original", "test.edu.standard-storage.tar"),
		ObjectIdentifier: "test.edu/standard-storage",
		StorageOption:    constants.StorageStandard,
		IsValidBag:       true,
		IsUpdate:         false,
		Files:            []*TestFile{},
	},
	&TestBag{
		PathToBag:        path.Join(testutil.PathToIntTestBags(), "original", "test.edu.wasabi-or.tar"),
		ObjectIdentifier: "test.edu/wasabi-or",
		StorageOption:    constants.StorageWasabiOR,
		IsValidBag:       true,
		IsUpdate:         false,
		Files:            []*TestFile{},
	},
	&TestBag{
		PathToBag:        path.Join(testutil.PathToIntTestBags(), "original", "test.edu.wasabi-va.tar"),
		ObjectIdentifier: "test.edu/wasabi-va",
		StorageOption:    constants.StorageWasabiVA,
		IsValidBag:       true,
		IsUpdate:         false,
		Files:            []*TestFile{},
	},
	&TestBag{
		PathToBag:        path.Join(testutil.PathToUnitTestBags(), "test.edu.btr-glacier-deep-oh.tar"),
		ObjectIdentifier: "test.edu/btr-glacier-deep-oh",
		StorageOption:    constants.StorageGlacierDeepOH,
		IsValidBag:       true,
		IsUpdate:         false,
		Files:            []*TestFile{},
	},
	&TestBag{
		PathToBag:        path.Join(testutil.PathToUnitTestBags(), "test.edu.btr-wasabi-or.tar"),
		ObjectIdentifier: "test.edu/btr-wasabi-or",
		StorageOption:    constants.StorageGlacierDeepOH,
		IsValidBag:       true,
		IsUpdate:         false,
		Files:            []*TestFile{},
	},
	&TestBag{
		PathToBag:        path.Join(testutil.PathToUnitTestBags(), "test.edu.btr_good_sha256.tar"),
		ObjectIdentifier: "test.edu/btr_good_sha256",
		StorageOption:    constants.StorageStandard,
		IsValidBag:       true,
		IsUpdate:         false,
		Files:            []*TestFile{},
	},
	&TestBag{
		PathToBag:        path.Join(testutil.PathToUnitTestBags(), "test.edu.btr_good_sha512.tar"),
		ObjectIdentifier: "test.edu/btr_good_sha512",
		StorageOption:    constants.StorageStandard,
		IsValidBag:       true,
		IsUpdate:         false,
		Files:            []*TestFile{},
	},

	/* ---- Invalid Bags ---- */

	&TestBag{
		PathToBag:        path.Join(testutil.PathToUnitTestBags(), "test.edu.btr_bad_checksums.tar"),
		ObjectIdentifier: "test.edu/btr_bad_checksums",
		StorageOption:    constants.StorageStandard,
		IsValidBag:       false,
		IsUpdate:         false,
		Files:            []*TestFile{},
	},
	&TestBag{
		PathToBag:        path.Join(testutil.PathToUnitTestBags(), "test.edu.btr_bad_extraneous_file.tar"),
		ObjectIdentifier: "test.edu/btr_bad_extraneous_file",
		StorageOption:    constants.StorageStandard,
		IsValidBag:       false,
		IsUpdate:         false,
		Files:            []*TestFile{},
	},
	&TestBag{
		PathToBag:        path.Join(testutil.PathToUnitTestBags(), "test.edu.btr_bad_missing_payload_file.tar"),
		ObjectIdentifier: "test.edu/btr_bad_missing_payload_file",
		StorageOption:    constants.StorageStandard,
		IsValidBag:       false,
		IsUpdate:         false,
		Files:            []*TestFile{},
	},
	&TestBag{
		PathToBag:        path.Join(testutil.PathToUnitTestBags(), "test.edu.btr_bad_missing_required_tags.tar"),
		ObjectIdentifier: "test.edu/btr_bad_missing_required_tags",
		StorageOption:    constants.StorageStandard,
		IsValidBag:       false,
		IsUpdate:         false,
		Files:            []*TestFile{},
	},
	&TestBag{
		PathToBag:        path.Join(testutil.PathToUnitTestBags(), "test.edu.sample_illegal_control.tar"),
		ObjectIdentifier: "test.edu/sample_illegal_control",
		StorageOption:    constants.StorageStandard,
		IsValidBag:       false,
		IsUpdate:         false,
		Files:            []*TestFile{},
	},

	/* ---- Updated bags ---- */

	&TestBag{
		PathToBag:        path.Join(testutil.PathToIntTestBags(), "updated", "test.edu.apt-001.tar"),
		ObjectIdentifier: "test.edu/apt-001",
		StorageOption:    constants.StorageStandard,
		IsValidBag:       true,
		IsUpdate:         true,
		Files:            []*TestFile{},
	},
	&TestBag{
		PathToBag:        path.Join(testutil.PathToIntTestBags(), "updated", "test.edu.apt-002.tar"),
		ObjectIdentifier: "test.edu/apt-002",
		StorageOption:    constants.StorageStandard,
		IsValidBag:       true,
		IsUpdate:         true,
		Files:            []*TestFile{},
	},
	&TestBag{
		PathToBag:        path.Join(testutil.PathToIntTestBags(), "updated", "test.edu.btr-001.tar"),
		ObjectIdentifier: "test.edu/btr-001",
		StorageOption:    constants.StorageStandard,
		IsValidBag:       true,
		IsUpdate:         true,
		Files:            []*TestFile{},
	},
	&TestBag{
		PathToBag:        path.Join(testutil.PathToIntTestBags(), "updated", "test.edu.btr-002.tar"),
		ObjectIdentifier: "test.edu/btr-002",
		StorageOption:    constants.StorageStandard,
		IsValidBag:       true,
		IsUpdate:         true,
		Files:            []*TestFile{},
	},
}
