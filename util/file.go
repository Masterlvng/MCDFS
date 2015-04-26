package util

import (
    "os"
    "time"
)

func CheckFile(filename string) (exists, canRead, canWrite bool, modTime time.Time) {
    exists = true
    fi, err := os.Stat(filename)
    if os.IsNotExist(err) {
        exists = false
        return
    }
    if fi.Mode()&0400 != 0 {
        canRead = true
    }
    if fi.Mode()&0200 != 0 {
        canWrite = true
    }
    modTime = fi.ModTime()
    return
}
