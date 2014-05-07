package command

import (
	"fmt"
	"github.com/goraft/raft"
	"storage"
)

type WriteCommand struct {
	Vid string
	N   []byte
}

type WriteRes struct {
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
	fmt.Printf("Get Needle %s", string(n.Name))
	v.Write(n)
	return WriteRes{n.Cookie, n.Offset, n.Size}, nil
}
