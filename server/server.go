package server

import (
	"github.com/Devying/db-agent/config"
	"net"
	"strconv"
)
var Conf *config.Config
func Init()  {
	Conf = config.ParseConf()
}
func Start() {
	var err error
	l,err := net.Listen("tcp",":"+strconv.Itoa(Conf.Port))
	if err != nil {
		panic(err)
	}
	db := NewDB()
	err = db.Config()
	if err != nil {
		panic(err)
	}
	err = db.Initialize()
	if err != nil {
		panic(err)
	}
	println("listing",Conf.Port)
	for{
		conn,err := l.Accept()
		if err != nil {
			panic(err)
		}
		go db.Process(conn)
	}
}