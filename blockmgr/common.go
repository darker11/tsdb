package blockmgr

import "fmt"

const (
	maxBlockAmountPerFile = 1024 // 4M 1024 * 4096
	maxFileAmount         = 65535
	blockSize             = 4096
	filePrefix            = "data"
	fileSuffix            = "db"
	defaultFilePath       = "./"
)

var (
	defaultFileID int64
)

func generateFileID() int64 {
	defaultFileID = defaultFileID + 1
	return defaultFileID
}

func joinFileName() string {
	return fmt.Sprintf("%s%s%d.%s", defaultFilePath, filePrefix, generateFileID(), fileSuffix)
}
