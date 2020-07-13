package server

import (
	"net"
)

type DB interface {
	Config() error
	Initialize() error
	Process(conn net.Conn)
}

func NewDB() DB {
	switch Conf.DB{
	case "redis":
		return &Redis{}
	case "mysql":
		return &Mysql{}
	}
	return &Redis{}
}
