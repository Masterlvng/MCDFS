package server

import (
	"bytes"
	"command"
	"encoding/json"
	"fmt"
	"github.com/goraft/raft"
	"github.com/gorilla/mux"
	"io/ioutil"
	"math/rand"
	"net/http"
	"path/filepath"
	"storage"
	"strconv"
	"sync"
	"time"
)

type Server struct {
	name       string
	host       string
	port       int
	path       string
	router     *mux.Router
	raftServer raft.Server
	httpServer *http.Server
	store      *storage.Store
	mutex      sync.Mutex
}

type WriteResult struct {
	Vid    int
	Offset uint64
	Size   uint32
	Cookie uint32
}

func New(path string, host string, port int, dirName []string) *Server {
	s := &Server{
		host:   host,
		port:   port,
		path:   path,
		store:  storage.NewStore(dirName),
		router: mux.NewRouter(),
	}
	s.store.AddVolume("1,2,3", "photo")
	if b, err := ioutil.ReadFile(filepath.Join(path, "name")); err == nil {
		s.name = string(b)
	} else {
		s.name = fmt.Sprintf("%07x", rand.Int())[0:7]
		if err = ioutil.WriteFile(filepath.Join(path, "name"), []byte(s.name), 0644); err != nil {
			panic(err)
		}
	}
	return s
}

func (s *Server) connectionString() string {
	return fmt.Sprintf("http://%s:%d", s.host, s.port)
}

func (s *Server) ListenAndServe(leader string) error {
	var err error
	t := raft.NewHTTPTransporter("/raft")
	s.raftServer, err = raft.NewServer(s.name, s.path, t, nil, s.store, "")
	if err != nil {
		fmt.Println(err.Error())
	}
	t.Install(s.raftServer, s)
	s.raftServer.Start()
	if leader != "" {
		s.Join(leader)

	} else if s.raftServer.IsLogEmpty() {
		r, err := s.raftServer.Do(&raft.DefaultJoinCommand{
			Name:             s.raftServer.Name(),
			ConnectionString: s.connectionString(),
		})
		fmt.Println(r)
		if err != nil {
			fmt.Println("do error")
		}
	}

	s.httpServer = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.port),
		Handler: s.router,
	}
	s.router.HandleFunc("/write", s.writeHandler).Methods("POST")
	s.router.HandleFunc("/read/{vid}/{offset}/{size}/{cookie}", s.readHandler).Methods("GET")
	s.router.HandleFunc("/join", s.joinHandler).Methods("POST")
	return s.httpServer.ListenAndServe()
}

func (s *Server) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	s.router.HandleFunc(pattern, handler)
}

func (s *Server) Join(leader string) error {
	command := &raft.DefaultJoinCommand{
		Name:             s.raftServer.Name(),
		ConnectionString: s.connectionString(),
	}
	var b bytes.Buffer
	json.NewEncoder(&b).Encode(command)
	resp, err := http.Post(fmt.Sprintf("http://%s/join", leader), "application/json", &b)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (s *Server) joinHandler(w http.ResponseWriter, req *http.Request) {
	command := &raft.DefaultJoinCommand{}
	if err := json.NewDecoder(req.Body).Decode(&command); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err := s.raftServer.Do(command); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *Server) readHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	vid, _ := storage.NewVolumeId(vars["vid"])
	v := s.store.GetVolume(vid)
	if v != nil {
		n := &storage.Needle{}
		n.Offset, _ = strconv.ParseUint(vars["offset"], 10, 64)
		size, _ := strconv.ParseUint(vars["size"], 10, 32)
		cookie, _ := strconv.ParseUint(vars["cookie"], 10, 32)
		n.Size = uint32(size)
		n.Cookie = uint32(cookie)
		v.Read(n)
		w.Write(n.Data)
        fmt.Println(len(n.Data))
	} else {
		http.Error(w, "vid not found", http.StatusBadRequest)
	}
}

func (s *Server) writeHandler(w http.ResponseWriter, req *http.Request) {
	filename, data, mimetype, _, _, e := storage.ParseUpload(req)
	if e != nil {
		w.Write([]byte("write error"))
	}
	size := len(data)
	n := &storage.Needle{}
	n.Cookie = rand.New(rand.NewSource(time.Now().UnixNano())).Uint32()
	n.Data = data
	n.DataSize = uint32(size)
	n.Name = []byte(filename)
	n.NameSize = uint8(len(n.Name))
	n.Mime = []byte(mimetype)
	n.MimeSize = uint8(len(n.Mime))
	n.LastModified = uint64(time.Now().Unix())
	n.SetHasMime()
	n.SetHasName()
	n.SetHasLastModifiedDate()
	n.Checksum = storage.NewCRC(n.Data)

	v := s.store.FreeVolume()

	bytes, err := n.GobEncode()
	if err != nil {
		fmt.Println("needle encode error")
	}
	rv, err := s.raftServer.Do(command.NewWriteCommand(v.Id.String(), bytes))
	if err == nil {
		res := rv.(command.WriteRes)
		w.Write([]byte(v.Id.String()))
		w.Write([]byte("\n"))
		content, _ := json.Marshal(res)
		w.Write([]byte("\n"))
		w.Write(content)
		return
	}
    leader := s.raftServer.Peers()[s.raftServer.Leader()].ConnectionString
	w.Write([]byte(leader))
}
