// -- go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"path"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/util/testutil"
)

/*
This file contains data used in end-to-end tests.
e2e_test.go pushes these bags through the ingest and restortation
processes and then tests for expected results.

This file is really only used in tests, but go complains if it
finds and e2e_test package without a corresponding e2e package.
*/

type TestBag struct {
	PathToBag        string
	ObjectIdentifier string
	StorageOption    string
	IsValidBag       bool
	IsUpdate         bool
	Files            []*TestFile
}

func (tb *TestBag) TarFileName() string {
	return path.Base(tb.PathToBag)
}

type TestFile struct {
	Identifier  string
	IsReingest  bool
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

// InitialBags returns a list of bags for initial ingest (i.e. not reingests)
func InitialBags() []*TestBag {
	bags := make([]*TestBag, 0)
	for _, tb := range TestBags {
		if tb.IsValidBag && !tb.IsUpdate {
			bags = append(bags, tb)
		}
	}
	return bags
}

// ReingestBags returns a list of bags for reingest (i.e. updated versions
// of initial ingests)
func ReingestBags() []*TestBag {
	bags := make([]*TestBag, 0)
	for _, tb := range TestBags {
		if tb.IsValidBag && tb.IsUpdate {
			bags = append(bags, tb)
		}
	}
	return bags
}

func LoadObjectJSON() ([]*registry.IntellectualObject, error) {
	data, err := testutil.ReadE2EFile("objects.json")
	if err != nil {
		return nil, err
	}
	objects := make([]*registry.IntellectualObject, 0)
	err = json.Unmarshal(data, &objects)
	return objects, err
}

func LoadGenericFileJSON() ([]*registry.GenericFile, error) {
	data, err := testutil.ReadE2EFile("files.json")
	if err != nil {
		return nil, err
	}
	files := make([]*registry.GenericFile, 0)
	err = json.Unmarshal(data, &files)
	return files, err
}

func GetObjectByIdentifier(objList []*registry.IntellectualObject, identifier string) (*registry.IntellectualObject, error) {
	if objList == nil {
		return nil, fmt.Errorf("Object list cannot be nil")
	}
	for _, obj := range objList {
		if obj.Identifier == identifier {
			return obj, nil
		}
	}
	return nil, fmt.Errorf("Object not found")
}

func GetFilesByObjectIdentifier(fileList []*registry.GenericFile, objIdentifier string) ([]*registry.GenericFile, error) {
	var err error
	files := make([]*registry.GenericFile, 0)
	if fileList == nil {
		return files, fmt.Errorf("File list cannot be nil")
	}
	for _, f := range fileList {
		ident, err := f.IntellectualObjectIdentifier()
		if err != nil {
			return files, err
		}
		if ident == objIdentifier {
			files = append(files, f)
		}
	}
	if len(files) == 0 {
		err = fmt.Errorf("No files found for object %s", objIdentifier)
	}
	return files, err
}

// -------- Restoration ----------

var FilesToRestore = []*TestFile{
	// Two original files
	&TestFile{
		Identifier: "test.edu/test.edu.wasabi-or/data/testbag/surfing.jpg",
		IsReingest: false,
		Size:       86006,
		Sha256:     "d3e7d17857c1d7d1e3bf47ef28f0b9b5e4359dab9a5f42aa93237347908d0425",
	},
	&TestFile{
		Identifier: "test.edu/test.edu.btr_good_sha256/data/netutil/listen_test.go",
		IsReingest: false,
		Size:       2141,
		Sha256:     "89c5b79f981601321d4fe9ebf49b44ac41fa1b2d0ec1cf02c2b24bb3bf12cd4a",
	},

	// Two updated files. Checksums should match LAST ingest
	&TestFile{
		Identifier: "test.edu/test.edu.apt-001/data/files/data.xml",
		IsReingest: true,
		Size:       24069,
		Sha256:     "a5cff0f39578dd62a0bf7669a1553b2b7ed10a3a8c804f6b9383b031afa9de02",
	},
	&TestFile{
		Identifier: "test.edu/test.edu.btr-001/data/files/data.json",
		IsReingest: true,
		Size:       20556,
		Sha256:     "03113acc50c634c28bbd9d606f356e2b58857668d160823835872acf0a0d7b9d",
	},
}

var BagsToRestore = []string{
	// Original bags
	"test.edu/test.edu.standard-storage",
	"test.edu/test.edu.btr_good_sha512",

	// Updated bags
	"test.edu/test.edu.apt-002",
	"test.edu/test.edu.btr-002",
}

// Run fixity checks on these files.
var FilesForFixityCheck = FilesToRestore

var FilesToDelete = []string{
	"test.edu/test.edu.glacier-va/data/testbag/surfing.jpg",
	"test.edu/test.edu.standard-storage/data/testbag/surfing.jpg",
}

var ObjectsToDelete = []string{
	"test.edu/test.edu.apt-002",
	"test.edu/test.edu.glacier-oh",
}
