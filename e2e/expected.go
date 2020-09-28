// +build e2e

package e2e

import (
	"path"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/util/testutil"
)

/*
This file contains data used in end-to-end tests.
e2e_test.go pushes these bags through the ingest and restortation
processes and then tests for expected results.
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

func LoadObjectJSON(t *testing.T) (map[string]*registry.IntellectualObject, error) {
	data, err := testutil.ReadE2EFile("objects.json")
	require.Nil(t, err)
	objects := make(map[string]*registry.IntellectualObject)
	err = json.Unmarshal(data, &objects)
	require.Nil(t, err)
	return objects
}

func LoadGenericFileJSON(t *testing.T) (map[string]*registry.GenericFile, error) {
	data, err := testutil.ReadE2EFile("files.json")
	require.Nil(t, err)
	files := make(map[string]*registry.GenericFile)
	err = json.Unmarshal(data, &files)
	require.Nil(t, err)
	return files
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
		if f.IntellectualObjectIdentifier == objIdentifier {
			files = append(files, f)
		}
	}
	if len(files) == 0 {
		err = fmt.Errorf("No files found for object %s", objIdentifier)
	}
	return files, err
}
