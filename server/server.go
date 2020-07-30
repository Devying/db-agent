package server

import (
	"flag"
	"github.com/Devying/db-agent/config"
	vlog "github.com/Devying/db-agent/log"
	"net"
	"strconv"
	"time"
)

func Init() {
	config.Conf = config.ParseConf()
	initLog(config.Conf.Log)
}
func Start() {
	var err error
	l, err := net.Listen("tcp", ":"+strconv.Itoa(config.Conf.Port))
	if err != nil {
		vlog.Infof("DB agent listening err: %s", err.Error())
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
	vlog.Infof("DB agent is starting.listening port: %d", config.Conf.Port)
	for {
		conn, err := l.Accept()
		if err != nil {
			panic(err)
		}
		go db.Process(conn)
	}
}

func initLog(logdir string) {
	// TODO: remove after a better handle
	if logdir == "stdout" {
		return
	}
	_ = flag.Set("log_dir", logdir)
	vlog.FlushInterval = 1 * time.Second
	vlog.LogInit(nil)
}
