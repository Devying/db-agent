package server

import (
	"bufio"
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"github.com/Devying/db-agent/config"
	"github.com/Devying/db-agent/third_party/mysql"
	"io"
	"net"
	"strconv"
	"time"
)

type Mysql struct {
	Conf   map[string]config.MysqlConfig
	Ins map[string]*MysqlInstance
}
type MysqlInstance struct {
	driver *mysql.MySQLDriver
	seq    byte
}
func (m *Mysql)Config() error{
	m.Conf = config.ParseMysqlConfig()
	return nil
}

func (m *Mysql) Initialize() error {
	m.Ins = make(map[string]*MysqlInstance)
	for k,v := range m.Conf{
		var err error
		dsn := v.User+":"+v.Pass+"@tcp("+v.Host+":"+strconv.Itoa(v.Port)+")/"+v.DB
		db,err := sql.Open("mysql", dsn)
		if err != nil  {
			return err
		}
		fmt.Println(v)
		db.SetMaxOpenConns(v.MaxOpenConn)
		db.SetMaxIdleConns(v.MaxIdleConn)
		db.SetConnMaxLifetime(3600 * time.Second)
		//go func() {
		//	tc := time.Tick(10 * time.Second)
		//	for {
		//		select {
		//		case <-tc:
					err = db.Ping()
					if err != nil  {
						fmt.Println(err)
					}
		//		}
		//	}
		//}()
		//_, err = db.Query("select id from task_user_0.users_0 limit 10")
		ins := &MysqlInstance{}
		mi,ok := db.Driver().(*mysql.MySQLDriver)
		if !ok {
			return errors.New("mysql driver error")
		}
		ins.driver = mi
		m.Ins[k] = ins
	}
	return nil
}
func (m *Mysql) Process(conn net.Conn) {
	//server 回复认证信息
	fmt.Println(conn.RemoteAddr(), "request")
	defer func() {
		_ = conn.Close()
		fmt.Println(conn.RemoteAddr(), "exit")
	}()
	var err error
	err = m.Auth(conn)
	if err != nil {
		//fmt.Println(err, "auth packet error")
		return
	}
	buf := bufio.NewReader(conn)
	dbName,err := m.ClientInfo(buf)
	if err != nil {
		//fmt.Println(err,"read client info error")
		return
	}

	insByte := bytes.Split(dbName,[]byte{'@'})
	if len(insByte)<2{
		_ = m.ErrResp(conn, "ins not specified")
		return
	}
	insName := string(insByte[len(insByte)-1])
	mi,ok := m.Ins[insName]
	if !ok {
		_ = m.ErrResp(conn, "ins not exists")
		return
	}
	err = m.AuthOK(conn)
	if err != nil {
		//fmt.Println(err, "client connect get lost")
		return
	}
	mct := mi.driver.GetConnector()
	if mct == nil {
		_ = m.ErrResp(conn, "server connector error")
		return
	}
	mc := mct.GetConn()
	if mc == nil {
		_ = m.ErrResp(conn, "server connect error")
		return
	}
	//开始处理client发过来的包
	defer func() {
		//重置seq
		mi.seq = 0
	}()
	for {
		clientData, err := mi.ReadPacket(buf)
		if err == io.EOF {
			return
		}
		if err != nil && err != io.EOF {
			//println("not EOF err")
			//fmt.Println(err)
			return
		}
		//向mysql server写入客户端发送的协议
		err = mc.WriteRawPacket(clientData)
		if err != nil {
			//agent 向server 写数据失败 需要给client 返回错误
			_ = m.ErrResp(conn, "send data to server error")
			continue
		}
		var serverData []byte
		var fieldLen int
		//首先需要读取首次数据报，以此判断是否需要读取多次。
		first, err := mc.ReadRawPacket()
		if err != nil {
			_ = m.ErrResp(conn, "receive data from server error")
			continue
		}
		serverData = append(serverData, first...)
		switch first[4] {
		case 0x00,0xfb,0xff://OK,,Err
			fieldLen = 0
		default:
			//Query
			firstPayload := first[4:]
			num, _, n := readLengthEncodedInteger(firstPayload)
			if n-len(firstPayload) == 0 {
				fieldLen = int(num)
			}
		}
		//Query
		if fieldLen > 0 {
			for i := 0; i <= fieldLen; i++ {
				fieldData, err := mc.ReadRawPacket()
				if err != nil {
					panic(err)
				}
				serverData = append(serverData, fieldData...)
			}
			for {
				data, err := mc.ReadRawPacket()
				if err != nil {
					//fmt.Println(err)
					break
				}
				serverData = append(serverData, data...)
				//错误标识
				if data[4] == 0xff {
					break
				}
				//结束标识
				if data[4] == 0xfe {
					break
				}
			}
		}
		_,_= conn.Write(serverData)
		//重置seq
		mi.seq = 0
	}

}

//读取客户端发送的mysql包
func (m *MysqlInstance) ReadPacket(buf io.Reader) ([]byte, error) {
	maxPacketSize := 1<<24 - 1
	var prevData []byte
	//var sequence byte
	for {
		//头部4个字节
		header := make([]byte, 4)
		n, err := io.ReadFull(buf, header)
		if err != nil {
			return nil, err
		}
		if n != 4 {
			return nil, errors.New("can not read header")
		}
		//根据前3个字节计算得出包长度
		pktLen := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
		//需要对比序号是否一致
		if header[3] != m.seq {
			if header[3] > m.seq {
				return nil, errors.New("read package seq error")
			}
			return nil, errors.New("read package seq error")
		}
		m.seq++
		prevData := append(prevData, header...)
		if pktLen == 0 {
			return prevData, nil
		}
		//body body的首位是有个传输结束控制符
		body := make([]byte, pktLen)
		n, err = io.ReadFull(buf, body)

		if err != nil || n != pktLen {
			return nil, err
		}
		if pktLen == 1 && body[0] == 1 {
			//客户端发送了Quit协议，我们需要拦截，因为我们是要复用链接
			return nil, errors.New("CLIENT SEND QUIT")
		}
		if pktLen < maxPacketSize {
			return append(prevData, body...), nil
		}
		prevData = append(prevData, body...)
	}
}

func (m *MysqlInstance) WritePacket(conn net.Conn, data []byte) error {
	maxAllowedPacket := 1<<24 - 1
	maxPacketSize := 1<<24 - 1
	pktLen := len(data) - 4

	if pktLen > maxAllowedPacket {
		return errors.New("package too large")
	}
	var sequence byte
	for {
		var size int
		if pktLen >= maxPacketSize {
			data[0] = 0xff
			data[1] = 0xff
			data[2] = 0xff
			size = maxPacketSize
		} else {
			data[0] = byte(pktLen)
			data[1] = byte(pktLen >> 8)
			data[2] = byte(pktLen >> 16)
			size = pktLen
		}
		//data[3] = sequence

		n, err := conn.Write(data[:4+size])
		if err == nil && n == 4+size {
			sequence++
			if size != maxPacketSize {
				return nil
			}
			pktLen -= size
			data = data[size:]
			continue
		}

		// Handle error
		if err == nil { // n != len(data)
			return errors.New("package write n != len")
		} else {
			if n == 0 && pktLen == len(data)-4 {
				// only for the first loop iteration when nothing was written yet
				return errors.New("package write none")
			}
		}
		return errors.New("conn error")
	}
}

func (m *Mysql) Auth(conn net.Conn) error {
	hash := []byte{78, 0, 0, 0, 10, 53, 46, 54, 46, 50, 57, 45, 108, 111, 103, 0, 27, 199, 0, 0, 116, 34, 68, 114, 51, 67, 58, 57, 0, 255, 247, 8, 2, 0, 127, 128, 21, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 77, 89, 88, 44, 113, 118, 33, 57, 60, 97, 83, 91, 0, 109, 121, 115, 113, 108, 95, 110, 97, 116, 105, 118, 101, 95, 112, 97, 115, 115, 119, 111, 114, 100, 0}
	_,err := conn.Write(hash)
	return err
}

func (m *Mysql) ClientInfo(buf io.Reader) ([]byte,error) {
	header := make([]byte, 4)
	n, err := io.ReadFull(buf, header)
	if err != nil {
		return nil,err
	}
	if n != 4 {
		return nil, errors.New("can not read header")
	}
	//根据前3个字节计算得出包长度
	pktLen := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
	//第4位为序号 server端最先发送了一个auth包序号为0 所以这里client回复的包序号肯定是1
	if header[3] != 1 {
		return nil, errors.New("read package seq error")
	}
	//body body的首位是有个传输结束控制符
	body := make([]byte, pktLen)
	n, err = io.ReadFull(buf, body)

	if err != nil{
		return nil, err
	}
	if n != pktLen {
		return nil,errors.New("read package body error")
	}
	if pktLen == 1 && body[0] == 1 {
		//客户端发送了Quit协议
		return nil, errors.New("client send quit")
	}
	var auth []byte
	auth =append(auth,header...)
	auth =append(auth,body...)
	//从第36个字节开始解析链接信息
	fmt.Println(string(auth))
	dbInfo := auth[36:]
	//分割协议(0x00分割)，第一段为用户名、第二段为密码+数据库名（其中第一个字节表示密码长度）
	db := bytes.Split(dbInfo,[]byte{0})
	if len(db)<2{
		return nil, errors.New("client error")
	}
	//没有指定db
	if len(db[1])== 0{
		return []byte("mysql@task"),nil
	}

	dbName := db[1][int(db[1][0]+1):]
	return dbName,nil
}
func (m *Mysql)AuthOK(conn net.Conn)error {
	//认证成功是第三部所以这里的包序号继续+1
	authOK := []byte{7, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0}
	_,err := conn.Write(authOK)
	return err
}
func (m *Mysql)ErrResp(conn net.Conn,msg string)error {
	//出现错误回复
	head := make([]byte,4)
	//1个0xff+2个错误代码
	pktLen := len(msg)+1+2
	head[0] = byte(pktLen)
	head[1] = byte(pktLen >> 8)
	head[2] = byte(pktLen >> 16)
	//建立连接后是第三部的开始所以这里序号是2
	head[3] = 2
	var data []byte
	data = append(data,head...)
	//0xff 表示错误状态
	data = append(data,0xff)//ERR_Packet标识
	//下面两行为错误号可以自定义
	data = append(data,0xff)
	data = append(data,0xff)
	//错误具体内容
	data = append(data,[]byte(msg)...)
	_,err := conn.Write(data)
	return err
}


func readLengthEncodedInteger(b []byte) (uint64, bool, int) {
	// See issue #349
	if len(b) == 0 {
		return 0, true, 1
	}
	switch b[0] {
	// 251: NULL
	case 0xfb:
		return 0, true, 1

	// 252: value of following 2
	case 0xfc:
		return uint64(b[1]) | uint64(b[2])<<8, false, 3

	// 253: value of following 3
	case 0xfd:
		return uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16, false, 4

	// 254: value of following 8
	case 0xfe:
		return uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16 |
				uint64(b[4])<<24 | uint64(b[5])<<32 | uint64(b[6])<<40 |
				uint64(b[7])<<48 | uint64(b[8])<<56,
			false, 9
	}

	// 0-250: value of first byte
	return uint64(b[0]), false, 1
}