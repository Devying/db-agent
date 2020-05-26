package server

import (
	"github.com/Devying/db-agent/config"
	"net"
	"strconv"
)
var Db DB
func Init()  {
	config.ParseConf()
	Db = GetDB()
	Db.Initialize()
}
func Start() {
	l,err := net.Listen("tcp",":"+strconv.Itoa(Db.GetPort()))
	if err != nil {
		panic(err)
	}
	println("listing",Db.GetPort())
	for{
		conn,err := l.Accept()
		if err != nil {
			panic(err)
		}
		go Db.Process(conn)
	}
}