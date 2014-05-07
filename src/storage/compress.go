package storage

import (
    "bytes"
    "compress/flate"
    "compress/gzip"
    "io/ioutil"
    "strings"
)

func IsGzippable(ext, mtype string) bool {
    if strings.HasPrefix(mtype, "text/") {
        return true
    }
    switch ext {
        case ".zip",".rar",".gz",".bz2",".xz":
            return false
        case ".pdf",".txt",".html",".css",".js",".json":
            return true
    }
    if strings.HasPrefix(mtype, "application/") {
        if strings.HasSuffix(mtype, "xml") {
            return true
        }
        if strings.HasSuffix(mtype, "script") {
            return true
        }
    }
    return false
}

func UnGzipData(data []byte) ([]byte, error) {
    buf := bytes.NewBuffer(data)
    r, _ := gzip.NewReader(buf)
    defer r.Close()
    output, err := ioutil.ReadAll(r)
    return output, err
}

func GzipData(data []byte) ([]byte, error) {
    buf := new(bytes.Buffer)
    w, _ := gzip.NewWriterLevel(buf, flate.BestCompression)
    if _, err := w.Write(data); err != nil {
        return nil, err
    }
    if err := w.Close(); err != nil {
        return nil, err
    }
    return buf.Bytes(), nil
}
