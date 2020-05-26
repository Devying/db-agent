package server

import (
	//"bufio"
	"github.com/Devying/db-agent/config"
	"errors"
	//"fmt"
	"github.com/Devying/db-agent/third_party/mysql"
	"io"
	"net"
)

type Mysql struct {
	Conf map[string]config.DBConf
	Port int
	Conn *mysql.MysqlConn
}
func (m *Mysql)GetPort()int{
	return m.Port
}
func (m *Mysql)Initialize(){
	md:= mysql.MySQLDriver{}
	var err error
	m.Conn,err = md.Open("ug_task:1q2w3e123@tcp(10.77.39.89:3306)/task_user_0")
	defer m.Conn.Close()
	if err != nil {
		panic(err)
	}

	//mysql 认证
	println("init====")
	//var err error
	//m.Conn,err = net.Dial("tcp","10.77.39.89:3306")
	//defer m.Conn.Close()
	//if err != nil {
	//	panic(err)
	//}
	//data,err := m.ReadPacket(bufio.NewReader(m.Conn))
	//if err != nil {
	//	panic(err)
	//}
	//fmt.Println(string(data))
	////接收server断发来的认证信息
	////读取4个字节 计算本次总的包大小
	//header := make([]byte,4)
	//buf := bufio.NewReader(conn)
	//n,err := io.ReadFull(buf,header)
	//if err != nil || n != 4 {
	//	panic(err)
	//}
	//size := int(uint32(header[0])|uint32(header[1]<<8)|uint32(header[2]<<16))
	//body := make([]byte,size)
	//n,err = io.ReadFull(buf,body)
	//if err != nil || n != size {
	//	panic(err)
	//}
	//fmt.Println(body)
	//fmt.Println(string(body))
	//conn.Close()
}
func (m *Mysql)Process(conn net.Conn){

	//buf := bufio.NewReader(conn)
	////hsresp := []byte{10,53, 46, 54, 46, 50, 57, 45, 108, 111, 103, 0, 215, 192, 0, 0, 119, 47, 125, 77, 69, 84, 85, 47, 0, 255, 247, 8, 2, 0, 127, 128, 21, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 43, 42, 47, 76, 115, 125, 61, 83, 107, 67, 74, 44, 0, 109, 121, 115, 113, 108, 95, 110, 97, 116, 105, 118, 101, 95, 112, 97, 115, 115, 119, 111, 114, 100, 0}
    //hs := []byte{78, 0, 0, 0, 10, 53, 46, 54, 46, 50, 57, 45, 108, 111, 103, 0, 27, 199, 0, 0, 116, 34, 68, 114, 51, 67, 58, 57, 0, 255, 247, 8, 2, 0, 127, 128, 21, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 77, 89, 88,44, 113, 118, 33, 57, 60, 97, 83, 91, 0,109, 121, 115, 113, 108, 95, 110, 97, 116, 105, 118, 101, 95, 112, 97, 115, 115, 119, 111, 114, 100,0}	//fmt.Println(conn.Write([]byte{}))
	////末尾最多4个
    //fmt.Println(conn.Write(hs))
    //println("reading")
	//pack := make([]byte,4096)
	//n,err := buf.Read(pack)
	//fmt.Println(int(uint32(pack[0])|uint32(pack[1]<<8)|uint32(pack[2]<<16)))
	//fmt.Println(n,err,string(pack))
	//auth := []byte{7,0, 0, 2, 0, 0, 0, 2, 0, 0, 0}
	//fmt.Println(conn.Write(auth))
	//buf.Read(pack)
}
//读取mysql包
func (m *Mysql)ReadPacket(buf io.Reader)([]byte, error){
	var data []byte
	var seq uint8
	for {
		header := make([]byte,4)
		println("block")
		n,err := io.ReadFull(buf,header)
		println("block over")
		if err != nil {
			if err == io.EOF{
				return data,nil
			}
			return nil,err
		}
		if n != 4 {
			return nil,errors.New("package header lost")
		}
		pkgLen := int(uint32(header[0])|uint32(header[1]<<8)|uint32(header[2]<<16))
		if seq != header[3]{
			return nil,errors.New("package seq error")
		}
		body := make([]byte,pkgLen)
		n,err = io.ReadFull(buf,body)
		if err != nil {
			if err == io.EOF{
				return data,nil
			}
			return nil,err
		}
		if n != pkgLen {
			return nil,errors.New("package body lost")
		}
		if pkgLen == 0 {
			if data ==nil {
				return nil,errors.New("package len 0")
			}
			return  data,nil
		}
		println("+++++")
		data = append(data,header...)
		data = append(data,body...)
		seq ++
	}
}