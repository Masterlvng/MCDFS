package command

import (
	"fmt"
	"github.com/Masterlvng/MCDFS/storage"
	"github.com/goraft/raft"
	"strconv"
)

type WriteCommand struct {
	Vid string
	N   []byte
}

type WriteRes struct {
	Vid    uint64
	Cookie uint32
	Offset uint64
	Size   uint32
}

func NewWriteCommand(id string, nbytes []byte) *WriteCommand {
	return &WriteCommand{
		Vid: id,
		N:   nbytes,
	}
}

func (c *WriteCommand) CommandName() string {
	return "write"
}

func (c *WriteCommand) Apply(server raft.Server) (interface{}, error) {
	s := server.Context().(*storage.Store)
	vid, _ := storage.NewVolumeId(c.Vid)
	v := s.GetVolume(vid)
	if v == nil {
		fmt.Printf("no volume %s\n", c.Vid)
		return nil, fmt.Errorf("no volume")
	}
	n := &storage.Needle{}
	err := n.GobDecode(c.N)
	if err != nil {
		fmt.Println(err.Error())
	}
	v.Write(n)
	uint64_vid, _ := strconv.ParseUint(v.Id.String(), 10, 10)
	return WriteRes{uint64_vid, n.Cookie, n.Offset, n.Size}, nil
}
