package main

import (
	"flag"
	"fmt"
	"github.com/Masterlvng/MCDFS/command"
	"github.com/Masterlvng/MCDFS/server"
	"github.com/goraft/raft"
	"math/rand"
	"os"
	"time"
)

var host string
var port int
var join string

var vlocation string

func init() {
	flag.StringVar(&host, "h", "localhost", "hostname")
	flag.IntVar(&port, "p", 4001, "port")
	flag.StringVar(&join, "join", "", "host:port of leader to join")
	flag.StringVar(&vlocation, "vl", "", "where to store volume")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [arguments] <data-path> \n", os.Args[0])
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())
	raft.RegisterCommand(&command.WriteCommand{})
	if flag.NArg() == 0 {
		flag.Usage()
	}
	path := flag.Arg(0)
	os.MkdirAll(path, 0744)
	var dirname []string
	dirname = append(dirname, vlocation)
	s := server.New(path, host, port, dirname)
	s.ListenAndServe(join)
}
