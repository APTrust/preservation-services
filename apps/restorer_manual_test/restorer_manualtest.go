package main

import (
	"fmt"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/restoration"
)

func main() {
	context := common.NewContext()
	restorationObject := &service.RestorationObject{
		Identifier:        "test.edu/apt-bag-1",
		RestorationSource: constants.RestorationSourceS3,
		RestorationTarget: "aptrust.restore.test.test.edu",
		RestorationType:   constants.RestorationTypeObject,
	}

	restorer := restoration.NewBagRestorer(
		context,
		9999,
		restorationObject,
	)

	fileCount, errors := restorer.Run()
	fmt.Println("FileCount:", fileCount)
	for _, e := range errors {
		fmt.Println(e.Error())
	}
}
