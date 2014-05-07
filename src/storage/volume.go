package storage

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"sync"
	"util"
)

type Volume struct {
	Id         VolumeId
	dir        string
	Collection string
	dataFile   *os.File
	counter    uint32
	readOnly   bool
	accessLock sync.Mutex
}

func (v *Volume) SetMemberForTest(dir string, file *os.File) {
	v.dir, v.dataFile = dir, file
}

func (v *Volume) FileName() (fileName string) {
	if v.Collection == "" {
		fileName = path.Join(v.dir, v.Id.String())
	} else {
		fileName = path.Join(v.dir, v.Collection+"_"+v.Id.String())
	}
	return
}

//读取或者创建文件
func (v *Volume) load() error {
	var e error
	fileName := v.FileName()
	if exists, canRead, canWrite, _ := util.CheckFile(fileName + ".dat"); exists && !canRead {
		return fmt.Errorf("cannot read dat file")
	} else if !exists || canWrite {
		v.dataFile, e = os.OpenFile(fileName+".dat", os.O_RDWR|os.O_CREATE, 0644)
	} else if exists && canRead {
		v.dataFile, e = os.Open(fileName + ".dat")
		v.readOnly = true
	} else {
		return fmt.Errorf("Unknown file")
	}
	if e != nil {
		if !os.IsPermission(e) {
			return fmt.Errorf("cannot load file")
		}
	}
	return e
}

func (v *Volume) Size() int64 {
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	stat, e := v.dataFile.Stat()
	if e == nil {
		return stat.Size()
	}
	return -1
}

func (v *Volume) Close() {
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	v.dataFile.Close()
}

func (v *Volume) isFileUnchanged(n *Needle) bool {
	if n.Offset > 0 {
		if _, err := v.dataFile.Seek(int64(n.Offset)*NeedlePaddingSize, 0); err != nil {
			return false
		}
		oldn := new(Needle)
		_, e := oldn.Read(v.dataFile, n.Size, n.Cookie)
		if e != nil {
			return false
		}
		if oldn.Checksum == n.Checksum && bytes.Equal(oldn.Data, n.Data) {
			n.Size = oldn.Size
			return true
		}
	}
	return false
}

func (v *Volume) Write(n *Needle) (size uint32, err error) {
	if v.readOnly {
		err = fmt.Errorf("read-only", v.dataFile)
		return
	}
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	if v.isFileUnchanged(n) {
		size = n.Size
		return
	}
	var offset int64
	if offset, err = v.dataFile.Seek(0, 2); err != nil {
		return
	}

	if offset%NeedlePaddingSize != 0 {
		offset = offset + (NeedlePaddingSize - offset%NeedlePaddingSize)
		if offset, err = v.dataFile.Seek(offset, 0); err != nil {
			return
		}
	}

	n.Offset = uint64(offset / NeedlePaddingSize)
	if size, err = n.Append(v.dataFile); err != nil {
		if e := v.dataFile.Truncate(offset); e != nil {
			err = fmt.Errorf("cannot truncate")
		}
		return
	}
	v.counter++
	return
}

func (v *Volume) delete(n *Needle) (uint32, error) {
	if v.readOnly {
		return 0, fmt.Errorf("%s is read-only")
	}
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	size := n.Size
	if _, err := v.dataFile.Seek(0, 2); err != nil {
		return size, err
	}
	n.Data = make([]byte, 0)
	_, err := n.Append(v.dataFile)
	return size, err

}

func (v *Volume) Read(n *Needle) (int, error) {
	v.accessLock.Lock()
	defer v.accessLock.Unlock()

	if _, err := v.dataFile.Seek(int64(n.Offset)*NeedlePaddingSize, 0); err != nil {
		return -1, err
	}
	return n.Read(v.dataFile, n.Size, n.Cookie)
}

func (v *Volume) Num() uint32 {
	v.accessLock.Lock()
	defer v.accessLock.Unlock()

	return v.counter
}

func NewVolume(dirname string, collection string, id VolumeId) (v *Volume, e error) {
	v = &Volume{dir: dirname, Collection: collection, Id: id}
	v.load()
	return
}
