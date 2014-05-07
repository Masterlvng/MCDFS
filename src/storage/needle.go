package storage

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"mime"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
	"util"
	//"encoding/hex"
)

const (
	NeedleHeaderSize   = 16 // Cookie + Offset + Size
	NeedlePaddingSize  = 8
	NeedleChecksumSize = 4
)

const (
	FlagGzip                = 0x01
	FlagHasName             = 0x02
	FlagHasMime             = 0x04
	FlagHasLastModifiedDate = 0x08
	LastModifiedBytesLength = 5
)

type Needle struct {
	Cookie uint32
	Offset uint64
	Size   uint32

	DataSize uint32
	Data     []byte
	Flags    byte

	NameSize     uint8
	Name         []byte
	MimeSize     uint8
	Mime         []byte
	LastModified uint64

	Checksum CRC
	Padding  []byte
}

func (n *Needle) GobEncode() ([]byte, error) {
	w := new(bytes.Buffer)
	encoder := gob.NewEncoder(w)
	encoder.Encode(n.Cookie)
	encoder.Encode(n.DataSize)
	encoder.Encode(n.Data)
	encoder.Encode(n.Flags)
	encoder.Encode(n.NameSize)
	encoder.Encode(n.Name)
	encoder.Encode(n.MimeSize)
	encoder.Encode(n.Mime)
	encoder.Encode(n.LastModified)
	err := encoder.Encode(n.Checksum.Value())
	return w.Bytes(), err
}

func (n *Needle) GobDecode(buf []byte) error {
	r := bytes.NewBuffer(buf)
	var checksum uint32
	decoder := gob.NewDecoder(r)
	decoder.Decode(&n.Cookie)
	decoder.Decode(&n.DataSize)
	decoder.Decode(&n.Data)
	decoder.Decode(&n.Flags)
	decoder.Decode(&n.NameSize)
	decoder.Decode(&n.Name)
	decoder.Decode(&n.MimeSize)
	decoder.Decode(&n.Mime)
	decoder.Decode(&n.LastModified)
	decoder.Decode(&checksum)
	if checksum == NewCRC(n.Data).Value() {
		return nil
	}
	return fmt.Errorf("error")
}

func (n *Needle) IsGzipped() bool {
	return n.Flags&FlagGzip > 0
}

func (n *Needle) SetGzipped() {
	n.Flags = n.Flags | FlagGzip
}

func (n *Needle) HasName() bool {
	return n.Flags&FlagHasName > 0
}

func (n *Needle) SetHasName() {
	n.Flags = n.Flags | FlagHasName
}

func (n *Needle) SetHasMime() {
	n.Flags = n.Flags | FlagHasMime
}

func (n *Needle) HasMime() bool {
	return n.Flags&FlagHasMime > 0
}

func (n *Needle) HasLastModifiedDate() bool {
	return n.Flags&FlagHasLastModifiedDate > 0
}

func (n *Needle) SetHasLastModifiedDate() {
	n.Flags = n.Flags | FlagHasLastModifiedDate
}

func (n *Needle) readNeedleHeader(bytes []byte) {
	n.Cookie = util.BytesToUint32(bytes[0:4])
	n.Offset = util.BytesToUint64(bytes[4:12])
	n.Size = util.BytesToUint32(bytes[12:NeedleHeaderSize])
}

func (n *Needle) readNeedleData(bytes []byte) {
	index, lenBytes := 0, len(bytes)
	if index < lenBytes {
		n.DataSize = util.BytesToUint32(bytes[index : index+4])
		index += 4
		n.Data = bytes[index : index+int(n.DataSize)]
		index += int(n.DataSize)
		n.Flags = bytes[index]
		index += 1
	}
	if index < lenBytes && n.HasName() {
		n.NameSize = uint8(bytes[index])
		index += 1
		n.Name = bytes[index : index+int(n.NameSize)]
		index += int(n.NameSize)
	}
	if index < lenBytes && n.HasMime() {
		n.MimeSize = uint8(bytes[index])
		index += 1
		n.Mime = bytes[index : index+int(n.MimeSize)]
		index += int(n.MimeSize)
	}
	if index < lenBytes && n.HasLastModifiedDate() {
		n.LastModified = util.BytesToUint64(bytes[index : index+LastModifiedBytesLength])
		index += LastModifiedBytesLength
	}
}

//r是用seek设置过offset的，利用这个接口，可以校验offset/size/cookie是否匹配
func ReadNeedleHeader(r *os.File) (n *Needle, bodyLength uint32, err error) {
	n = new(Needle)
	bytes := make([]byte, NeedleHeaderSize)
	var count int
	count, err = r.Read(bytes)
	if count <= 0 || err != nil {
		return nil, 0, err
	}
	n.readNeedleHeader(bytes)
	padding := NeedlePaddingSize - ((n.Size + NeedleHeaderSize + NeedleChecksumSize) % NeedlePaddingSize)
	bodyLength = n.Size + NeedleChecksumSize + padding
	return
}

func (n *Needle) generateCookie() {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	n.Cookie = r.Uint32()
}

func (n *Needle) DiskSize() int64 {
	padding := NeedlePaddingSize - ((NeedleHeaderSize + int64(n.Size) + NeedleChecksumSize) % NeedlePaddingSize)
	return NeedleHeaderSize + int64(n.Size) + NeedleChecksumSize + padding
}

//当needle结构的成员都有值（除size外），才调用此接口。完成后返回size的值
func (n *Needle) Append(w io.Writer) (size uint32, err error) {
	if s, ok := w.(io.Seeker); ok {
		if end, e := s.Seek(0, 1); e == nil {
			defer func(s io.Seeker, off int64) {
				if err != nil {
					s.Seek(off, 0)
				}
			}(s, end)
		} else {
			err = fmt.Errorf("Canot read current volume Position: %s", e.Error())
			return
		}
	}
	header := make([]byte, NeedleHeaderSize)

	//n.generateCookie()

	util.Uint32toBytes(header[0:4], n.Cookie)
	util.Uint64toBytes(header[4:12], n.Offset)
	n.DataSize, n.NameSize, n.MimeSize = uint32(len(n.Data)), uint8(len(n.Name)), uint8(len(n.Mime))
	if n.DataSize > 0 {
		n.Size = 4 + n.DataSize + 1
		if n.HasName() {
			n.Size = n.Size + 1 + uint32(n.NameSize)
		}
		if n.HasMime() {
			n.Size = n.Size + 1 + uint32(n.MimeSize)
		}
		if n.HasLastModifiedDate() {
			n.Size = n.Size + LastModifiedBytesLength
		}
	}
	size = n.DataSize
	util.Uint32toBytes(header[12:16], n.Size)
	if _, err = w.Write(header); err != nil {
		return
	}
	if n.DataSize > 0 {
		util.Uint32toBytes(header[0:4], n.DataSize)
		if _, err = w.Write(header[0:4]); err != nil {
			return
		}
		if _, err = w.Write(n.Data); err != nil {
			return
		}
		util.Uint8toBytes(header[0:1], n.Flags)
		if _, err = w.Write(header[0:1]); err != nil {
			return
		}
		if n.HasName() {
			util.Uint8toBytes(header[0:1], n.NameSize)
			if _, err = w.Write(header[0:1]); err != nil {
				return
			}
			if _, err = w.Write(n.Name); err != nil {
				return
			}
		}
		if n.HasMime() {
			util.Uint8toBytes(header[0:1], n.MimeSize)
			if _, err = w.Write(header[0:1]); err != nil {
				return
			}
			if _, err = w.Write(n.Mime); err != nil {
				return
			}
		}
		if n.HasLastModifiedDate() {
			util.Uint64toBytes(header[0:8], n.LastModified)
			if _, err = w.Write(header[8-LastModifiedBytesLength : 8]); err != nil {
				return
			}
		}
		padding := NeedlePaddingSize - ((NeedleHeaderSize + n.Size + NeedleChecksumSize) % NeedlePaddingSize)
		util.Uint32toBytes(header[0:NeedleChecksumSize], n.Checksum.Value())
		_, err = w.Write(header[0 : NeedleChecksumSize+padding])
		return n.DataSize, err
	}
	return 0, fmt.Errorf("error!")
}

//r是经过seek调用，已经偏移header
func (n *Needle) ReadNeedleBody(r *os.File, bodyLength uint32) (err error) {
	if bodyLength <= 0 {
		return nil
	}
	bytes := make([]byte, bodyLength)
	if _, err = r.Read(bytes); err != nil {
		return
	}
	n.readNeedleData(bytes[0:n.Size])
	n.Checksum = NewCRC(n.Data)
	return
}

//size 是needle的size
func (n *Needle) Read(r io.Reader, size uint32, cookie uint32) (ret int, err error) {
	if size == 0 {
		return 0, nil
	}
	bytes := make([]byte, NeedleHeaderSize+size+NeedleChecksumSize)
	if ret, err = r.Read(bytes); err != nil {
		return
	}
	if ret != int(NeedleHeaderSize+size+NeedleChecksumSize) {
		fmt.Printf("%d\n%d\n", ret, int(NeedleHeaderSize+size+NeedleChecksumSize))
		return 0, fmt.Errorf("File Entry Not Found")
	}
	n.readNeedleHeader(bytes)
	if n.Size != size || n.Cookie != cookie {
		fmt.Println(n.Offset)
		fmt.Println(n.Cookie)
		return 0, fmt.Errorf("File Entry Not Found cookie")
	}
	n.readNeedleData(bytes[NeedleHeaderSize : NeedleHeaderSize+n.Size])
	checksum := util.BytesToUint32(bytes[NeedleHeaderSize+n.Size : NeedleHeaderSize+n.Size+NeedleChecksumSize])
	newchecksum := NewCRC(n.Data)
	if checksum != newchecksum.Value() {
		return 0, fmt.Errorf("CRC error")
	}
	n.Checksum = newchecksum
	return
}

func ParseUpload(r *http.Request) (fileName string, data []byte, mimeType string, isGzipped bool, modifiedTime uint64, e error) {
	form, _ := r.MultipartReader()
	part, _ := form.NextPart()
	fileName = part.FileName()
	data, e = ioutil.ReadAll(part)
	dotIndex := strings.LastIndex(fileName, ".")
	var ext, mtype string
	if dotIndex > 0 {
		ext = strings.ToLower(fileName[dotIndex:])
		mtype = mime.TypeByExtension(ext)
	}
	contentType := part.Header.Get("Content-Type")
	if contentType != "" && mtype != contentType {
		mimeType = contentType
		mtype = contentType
	}

	if part.Header.Get("Content-Encoding") == "gzip" {
		isGzipped = true
	} else if IsGzippable(ext, mtype) {
		data, _ = GzipData(data)
		isGzipped = true
	}

	if ext == ".gz" {
		isGzipped = true
	}
	if strings.HasSuffix(fileName, ".gz") {
		fileName = fileName[:len(fileName)-3]
	}
	modifiedTime, _ = strconv.ParseUint(r.FormValue("ts"), 10, 64)
	return
}

func NewNeedle(r *http.Request) (n *Needle, e error) {
	n = &Needle{}
	var name, mtype string
	var isGzipped bool
	name, n.Data, mtype, isGzipped, n.LastModified, e = ParseUpload(r)
	if e != nil {
		return
	}
	if len(name) < 256 {
		n.Name = []byte(name)
		n.SetHasName()
	}
	if len(mtype) < 256 {
		n.Mime = []byte(mtype)
		n.SetHasMime()
	}
	if isGzipped {
		n.SetGzipped()
	}
	if n.LastModified == 0 {
		n.LastModified = uint64(time.Now().UnixNano())
	}
	n.SetHasLastModifiedDate()
	n.Checksum = NewCRC(n.Data)
	commaindex := strings.LastIndex(r.URL.Path, ",")
	dotindex := strings.LastIndex(r.URL.Path, ".")
	fid := r.URL.Path[commaindex+1:]
	if dotindex > 0 {
		fid = r.URL.Path[commaindex+1 : dotindex]
	}
	e = n.ParseUploadPath(fid)
	return
}

func (n *Needle) ParseUploadPath(fid string) (err error) {
	delta := ""
	deltaIndex := strings.LastIndex(fid, "_")
	if deltaIndex > 0 {
		fid, delta = fid[0:deltaIndex], fid[deltaIndex+1:]
	}
	n.Offset, n.Cookie, err = ParseOffsetCookie(fid)
	if err != nil {
		return err
	}
	if delta != "" {
		if d, e := strconv.ParseUint(delta, 10, 64); e == nil {
			n.Offset += d
		} else {
			return e
		}
	}
	return err
}

/*
func ParseKeyHash(key_hash_string string) (uint64, uint32, error) {
    key_hash_bytes, khe := hex.DecodeString(key_hash_string)
    key_hash_len := len(key_hash_bytes)
    if khe != nil || key_hash_len <= 4 {
        return 0, 0, fmt.Errorf("Invalid key and hash")
    }
    key := util.BytesToUint64(key_hash_bytes[0:key_hash_len-4])
    hash := util.BytesToUint32(key_hash_bytes[key_hash_len-4:key_hash_len])
    return key, hash, nil
}
*/

func ParseOffsetCookie(offset_cookie_string string) (uint64, uint32, error) {
	s := strings.Split(offset_cookie_string, "/")
	if len(s) != 2 {
		return 0, 0, fmt.Errorf("Invalid offset size and cookie")
	}
	offset, _ := strconv.ParseUint(s[0], 10, 64)
	cookie, _ := strconv.ParseUint(s[1], 10, 32)
	return offset, uint32(cookie), nil
}

func ParseOffsetSize(offset_size_string string) (uint64, uint32, uint32, error) {
	s := strings.Split(offset_size_string, "/")
	if len(s) != 3 {
		return 0, 0, 0, fmt.Errorf("Invalid offset size and cookie")
	}
	offset, _ := strconv.ParseUint(s[0], 10, 64)
	size, _ := strconv.ParseUint(s[1], 10, 32)
	cookie, _ := strconv.ParseUint(s[2], 10, 32)
	return offset, uint32(size), uint32(cookie), nil
}
