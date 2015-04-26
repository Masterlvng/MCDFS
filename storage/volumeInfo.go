package storage

type VolumeInfo struct {
    Id  VolumeId
    Size uint64
    Collection string
    FileCount int
    DeleteCount int
    DeletedByteCount uint64
    ReadOnly bool
}
