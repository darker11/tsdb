package blockmgr

import (
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func getFillBuffer() []byte {
	data := make([]byte, blockSize)
	for i := 0; i < blockSize; i++ {
		data[i] = byte(i)
	}
	return data
}

func TestBlock(t *testing.T) {
	Convey("block test equal and less than and more than block max size data insert", t, func() {
		b := NewBlock()
		So(b.err, ShouldBeNil)
		data := make([]byte, blockSize)
		writeAmount, writeBlockIDSet := b.write(data)
		So(writeBlockIDSet, ShouldResemble, []int64{1})
		So(writeAmount, ShouldEqual, blockSize)

		data = make([]byte, blockSize-1)
		writeAmount, writeBlockIDSet = b.write(data)
		So(writeAmount, ShouldEqual, blockSize-1)
		So(writeBlockIDSet, ShouldResemble, []int64{1})

		data = make([]byte, blockSize+1)
		writeAmount, writeBlockIDSet = b.write(data)
		So(writeAmount, ShouldEqual, blockSize+1)
		So(writeBlockIDSet, ShouldResemble, []int64{2, 3})

		fileInfo, _ := os.Lstat(b.fileNameSet[0])
		So(fileInfo.Size(), ShouldEqual, blockSize*3)
		b.delete()
	})
	Convey("block test split file storage data", t, func() {
		b := NewBlock()
		So(b.err, ShouldBeNil)
		for i := 0; i < maxBlockAmountPerFile; i++ {
			data := make([]byte, blockSize)
			b.write(data)
		}

		So(b.blockID, ShouldEqual, maxBlockAmountPerFile)
		fileInfo, _ := os.Lstat(b.fileNameSet[0])
		So(fileInfo.Size(), ShouldEqual, blockSize*1024)

		data := make([]byte, blockSize+1)
		writeAmount, _ := b.write(data)
		So(writeAmount, ShouldEqual, blockSize+1)
		So(b.blockID, ShouldEqual, maxBlockAmountPerFile+1)

		fileInfo, _ = os.Lstat(b.fileNameSet[1])
		So(fileInfo.Size(), ShouldEqual, blockSize)

		So(len(b.cacheData), ShouldEqual, 1)
		b.delete()

	})
	Convey("block test read data by block Id", t, func() {
		b := NewBlock()
		So(b.err, ShouldBeNil)
		pendingWriteData := getFillBuffer()
		writeAmount, writeBlockIDSet := b.write(pendingWriteData)
		So(writeBlockIDSet, ShouldResemble, []int64{1})
		So(writeAmount, ShouldEqual, blockSize)

		readData := b.read(writeBlockIDSet[0])
		So(b.err, ShouldBeNil)
		So(readData, ShouldResemble, pendingWriteData)
		b.delete()

	})
	Convey("block test read data by block Id set", t, func() {
		b := NewBlock()
		So(b.err, ShouldBeNil)
		var (
			data       = make([]byte, 0)
			blockIDSet = make([]int64, 0)
		)
		for i := 0; i < 3; i++ {
			pendingWriteData := getFillBuffer()
			_, writeBlockIDSet := b.write(pendingWriteData)
			data = append(data, pendingWriteData...)
			blockIDSet = append(blockIDSet, writeBlockIDSet...)

		}
		So(blockIDSet, ShouldResemble, []int64{1, 2, 3})
		So(len(data), ShouldEqual, blockSize*3)

		readData := b.ReadByBlockIDSet(blockIDSet)
		So(b.err, ShouldBeNil)
		So(readData, ShouldResemble, data)
		b.delete()

	})
}
