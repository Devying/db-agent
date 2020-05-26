package server

import (
	"github.com/Devying/db-agent/config"
	"net"
)

type DB interface {
	Initialize()
	Process(conn net.Conn)
	GetPort()int
}
func GetDB() DB {
	switch config.Conf.Server.DB {
	case "redis":
		return &Redis{
			Conf:config.Conf.Databases,
			Port:config.Conf.Server.Port,
		}
	case "mysql":
		return &Mysql{
			Conf:config.Conf.Databases,
			Port:config.Conf.Server.Port,
		}
	}
	return &Redis{}
}