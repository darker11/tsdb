package blockmgr

import (
	"errors"
	"fmt"
	"os"
	"sort"
)

type BlockMgr interface {
	Write(data []byte) (int64, []int64)
	Read(blockID int64) []byte
}

type Block struct {
	cacheData   []byte
	size        int64
	blockID     int64
	fileID      *os.File
	fileNameSet []string
	err         error
}

func NewBlock() *Block {
	fName := joinFileName()
	fd, err := os.OpenFile(fName, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return &Block{
			err: err,
		}
	}
	return &Block{
		cacheData:   make([]byte, 0),
		size:        0,
		blockID:     0,
		fileID:      fd,
		fileNameSet: []string{fName},
		err:         nil,
	}
}

// 检查是否需要分割文件，预防写入数据超过限定的文件大小
func (b *Block) checkFileSize() bool {
	if b.size+int64(len(b.cacheData)) == maxBlockAmountPerFile*blockSize {
		return true
	}
	return false
}

func (b *Block) resetFileInfo() {
	fName := joinFileName()
	fd, err := os.OpenFile(fName, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		b.err = err
	}
	b.fileNameSet = append(b.fileNameSet, fName)
	b.fileID = fd
	b.size = 0
}

func (b *Block) flushCacheData() (int64, error) {
	if len(b.cacheData) != blockSize {
		return 0, errors.New("cache data not need flush")
	}
	splitFile := b.checkFileSize()
	writeAmount, err := b.fileID.Write(b.cacheData)
	if err != nil {
		return 0, err
	}
	if writeAmount != blockSize {
		return 0, errors.New("write cache data is not complete")
	}
	if splitFile {
		b.resetFileInfo()
		if b.err != nil {
			return 0, b.err
		}
	} else {
		b.size += int64(writeAmount)
	}
	b.cacheData = b.cacheData[:0]
	//todo 设置 blockID 的最大值，限制单个 block 对象可存储的数据大小
	b.blockID++
	return b.blockID, nil
}

func (b *Block) Write(data []byte) (int64, []int64) {
	var (
		writeAmount int64
		blockIDSet  = make([]int64, 0)
	)
	for int64(len(data))+int64(len(b.cacheData)) >= blockSize {
		// 临界点
		segementSize := blockSize - len(b.cacheData)
		b.cacheData = append(b.cacheData, data[:segementSize]...)
		// todo 写失败，需要全部回滚，返回 0 不是好的处理方式
		blockID, err := b.flushCacheData()
		if err != nil {
			b.err = err
			return 0, blockIDSet
		}
		blockIDSet = append(blockIDSet, blockID)
		data = data[segementSize:]
		writeAmount += int64(segementSize)
	}
	// 临界点
	b.cacheData = append(b.cacheData, data...)
	writeAmount += int64(len(data))
	// 没有触发缓存数据写入操作，返回当前的 blockID
	if len(blockIDSet) == 0 {
		blockIDSet = append(blockIDSet, b.blockID)
	}
	return writeAmount, blockIDSet
}

func (b *Block) getFileNameByBlockID(blockID int64) string {
	return b.fileNameSet[blockID/maxBlockAmountPerFile]
}
func getReadOffset(blockID int64) int64 {
	return ((blockID % maxBlockAmountPerFile) - 1) * blockSize
}

func (b *Block) ReadByBlockIDSet(blockIDSet []int64) []byte {
	sort.Slice(blockIDSet, func(i, j int) bool { return blockIDSet[i] < blockIDSet[j] })
	var (
		data        = make([]byte, 0)
		oldFileName string
		curFileName string
		err         error
		fd          *os.File
	)
	for index, blockID := range blockIDSet {
		curFileName = b.getFileNameByBlockID(blockID)
		if index == 0 {
			oldFileName = b.getFileNameByBlockID(blockID)
		} else {
			oldFileName = b.getFileNameByBlockID(blockIDSet[index-1])
		}
		if curFileName != oldFileName || fd == nil {
			if fd != nil {
				err = fd.Close()
				if err != nil {
					b.err = err
					return nil
				}
			}
			fd, err = os.Open(curFileName)
			if err != nil {
				b.err = err
				return nil
			}
		}
		offset := getReadOffset(blockID)
		tmpData := make([]byte, blockSize)
		readAmount, err := fd.ReadAt(tmpData, offset)
		if err != nil {
			b.err = err
			return nil
		}
		if readAmount != blockSize {
			b.err = errors.New("read data is not complete")
			return nil
		}
		data = append(data, tmpData...)
	}
	fd, err = os.Open(curFileName)
	if err != nil {
		b.err = err
		return nil
	}
	return data
}

func (b *Block) Read(blockID int64) []byte {
	fd, err := os.Open(b.getFileNameByBlockID(blockID))
	if err != nil {
		b.err = err
		return nil
	}
	offset := getReadOffset(blockID)
	data := make([]byte, blockSize)
	readAmount, err := fd.ReadAt(data, offset)
	if err != nil {
		b.err = err
		return nil
	}
	if readAmount != blockSize {
		b.err = errors.New("read data is not complete")
		return nil
	}
	err = fd.Close()
	if err != nil {
		b.err = err
		return nil
	}
	return data
}

func (b *Block) delete() {
	for _, fileName := range b.fileNameSet {
		err := os.Remove(fileName)
		if err != nil {
			fmt.Println("remove file failed", err, fileName)
		}

	}
}
