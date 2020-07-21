package server

import (
	"github.com/Devying/db-agent/config"
	"net"
)

type DB interface {
	Config() error
	Initialize() error
	Process(conn net.Conn)
}

func NewDB() DB {
	switch config.Conf.DB{
	case "redis":
		return &Redis{}
	case "mysql":
		return &Mysql{}
	}
	return &Redis{}
}
