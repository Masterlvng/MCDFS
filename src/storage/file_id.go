package storage

import (
    "strings"
    "strconv"
)

type FileId struct {
    VolumeId VolumeId
    Offset uint64
    Size uint32
    Cookie uint32
}

func NewFileId(VolumeId VolumeId, Offset uint64, Size uint32, Cookie uint32) *FileId {
    return &FileId{VolumeId: VolumeId, Offset: Offset, Size: Size, Cookie:Cookie}
}

func ParseFileId(fid string) (*FileId, error) {
    a := strings.Split(fid, ",")
    vid_string, offset_string := a[0], a[1]
    volumeId, e := NewVolumeId(vid_string)
    offset, size, cookie, _ := ParseOffsetSize(offset_string)
    return &FileId{VolumeId: volumeId, Offset: offset, Size: size, Cookie: cookie}, e
}

func (n *FileId) String() string {
    return n.VolumeId.String() + "," + strconv.FormatUint(n.Offset, 10) + "/" + strconv.FormatUint(uint64(n.Size), 10) + "/" + strconv.FormatUint(uint64(n.Cookie), 10)
}
