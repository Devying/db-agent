package server

import (
	"fmt"
	"github.com/Devying/db-agent/config"
	"github.com/Devying/db-agent/pkg/mysqlagent"
	"net"
)
var conf map[string]config.MysqlConfig

type Mysql struct {
	Instance map[string]mysqlagent.Instance
}
func (m *Mysql)Config() error{
	conf = config.ParseMysqlConfig()
	return nil
}
func (m *Mysql) Initialize() error {
	//初始化
	m.Instance = make(map[string]mysqlagent.Instance)
	for k,v := range conf{
		fmt.Println(v)
		var ins =  mysqlagent.Instance{}
		ins.Conf = v
		ins.Initialize()
		m.Instance[k] = ins
	}
	return nil
}
func (m *Mysql) Process(conn net.Conn) {
	//server 回复认证信息
	defer func() {
		_ = conn.Close()
		fmt.Println(conn.RemoteAddr(), "exit")
	}()
	var err error
	err = mysqlagent.Handshake(conn)
	if err != nil {
		fmt.Println(err, "handshake packet error")
		return
	}
	insInfo,err := mysqlagent.GetInstance(conn)
	if err != nil {
		fmt.Println(err,"read client info error")
		return
	}
	insName := string(insInfo)
	ins,ok := m.Instance[insName]
	if !ok {
		_ = mysqlagent.Err(conn, 2,"ins not exists")
		return
	}
	err = ins.AuthOK(conn)
	if err != nil {
		//fmt.Println(err, "client connect get lost")
		return
	}
	ins.Handle(conn)
}




