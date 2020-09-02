package mysqlagent

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/Devying/db-agent/config"
	"io"
	"net"
	"strconv"
)

type Instance struct {
	Pool chan InstanceConn
	Conf config.MysqlConfig
	seq byte
}
func (ins *Instance)Initialize(){
	pool := make(chan InstanceConn,ins.Conf.MaxPoolSize)
	for i:=0;i<ins.Conf.InitPoolSize;i++{
		var conn InstanceConn
		err := conn.Connect(ins.Conf.User+":"+ins.Conf.Pass+"@tcp("+ins.Conf.Host+":"+strconv.Itoa(ins.Conf.Port)+")/"+ins.Conf.DB)
		if err != nil {
			panic(err)
		}
		pool <- conn
	}
	ins.Pool = pool
}
func GetInstance(conn net.Conn) ([]byte, error) {
	buf := bufio.NewReader(conn)
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
	//第4位为序号 server端最先发送了一个handshake包序号为0 所以这里client回复的包序号肯定是1
	if header[3] != 1 {
		return nil, errors.New("read package seq error")
	}
	//body body的首位是有个传输结束控制符
	body := make([]byte, pktLen)
	n, err = io.ReadFull(buf, body)

	if err != nil {
		return nil, err
	}
	if n != pktLen {
		return nil, errors.New("read package body error")
	}
	if pktLen == 1 && body[0] == 1 {
		//客户端发送了Quit协议
		return nil, errors.New("client send quit")
	}
	var auth []byte
	auth = append(auth, header...)
	auth = append(auth, body...)
	//4.1后从第36个字节开始解析链接信息
	//fmt.Println(string(auth))
	//capabilities := auth[4:8]
	//
	//fmt.Println(capabilities)
	dbInfo := auth[36:]
	//分割协议(0x00分割)，第一段为用户名、第二段为认证信息+数据库名（其中第一个字节表示认证信息长度）
	db := bytes.Split(dbInfo, []byte{0x00})
	if len(db) < 2 {
		return nil, errors.New("client error")
	}
	//没有指定db
	if len(db) <= 3 {
		return []byte("dft"), nil
	}
	dbName := db[1][int(db[1][0]+1):]
	if len(dbName) == 0 {
		return []byte("dft"), nil
	}
	//fmt.Println("dbName:",dbName,string(dbName))
	return dbName, nil
}

//读取客户端发送的mysql包
func (ins *Instance) ReadPacket(buf io.Reader) ([]byte, error) {
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
		if header[3] != ins.seq {
			if header[3] > ins.seq {
				return nil, errors.New("read package seq error")
			}
			return nil, errors.New("read package seq error")
		}
		ins.seq++
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
		//if pktLen == 1 && body[0] == 1 {
		//	//客户端发送了Quit协议，我们需要拦截，因为我们是要复用链接
		//	return nil, errors.New("CLIENT SEND QUIT")
		//}
		if pktLen < maxPacketSize {
			return append(prevData, body...), nil
		}
		prevData = append(prevData, body...)
	}
}

func (ins *Instance) WritePacket(conn net.Conn, data []byte) error {
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

func (ins *Instance) Handle(conn net.Conn)  {
	//拿到一个Pool
	pool,err := ins.GetPool()
	if err != nil {
		fmt.Println(err)
	}
	mc := pool.md.MC

	//开始处理client发过来的包
	defer func() {
		//重置seq
		ins.ResetSeq()
		_ = ins.PutPool(pool)
		//_= pool.driver.GetConnector().GetConn().Close()
	}()
	buf := bufio.NewReader(conn)
	fmt.Println("process========>",conn.RemoteAddr())
	for {
		clientData, err := ins.ReadPacket(buf)
		fmt.Println("send:",clientData,string(clientData))
		if err == io.EOF {
			//fmt.Println("send:EOF err")
			return
		}
		if err != nil && err != io.EOF {
			//println("not EOF err")
			fmt.Println(err)
			return
		}
		if mc == nil {
			_ = Err(conn,2,"mysql connect error")
		}
		//向mysql server写入客户端发送的协议
		err = mc.WriteRawPacket(clientData)
		if err != nil {
			//agent 向server 写数据失败 需要给client 返回错误
			fmt.Println(err)
			_ = Err(conn,2, "send data to server error")
			continue
		}
		//var serverData []byte
		var fieldLen int
		//首先需要读取首次数据报，以此判断是否需要读取多次。
		first, err := mc.ReadRawPacket()
		//fmt.Println("first:",first,string(first))
		//立即回写
		_,_= conn.Write(first)
		if err != nil {
			fmt.Println(err)
			_ = Err(conn,2,"receive data from server error")
			continue
		}

		command := clientData[4]
		//serverData = append(serverData, first...)
		//STMT CLOSE
		switch command {
		case comQuit,comStmtClose:
				return
		case comStmtPrepare:
			//https://dev.mysql.com/doc/internals/en/com-stmt-prepare-response.html#packet-COM_STMT_PREPARE_OK
			//根据first包解析是否需要继续读取
			// Column count [16 bit uint]
			columnCount := binary.LittleEndian.Uint16(first[9:11])
			// Param count [16 bit uint]
			paramCount := int(binary.LittleEndian.Uint16(first[11:13]))
			fmt.Println("columnCount:",columnCount,"paramCount:",paramCount)
			if columnCount > 0 {
				for{
					columnData, err := mc.ReadRawPacket()
					if err != nil {
						fmt.Println(err)
						_ = Err(conn, 2,"receive data from server error")
						continue
					}
					conn.Write(columnData)
					//serverData = append(serverData, columnData...)
					if columnData[4]==0xfe || columnData[4]==0xff{
						break
					}
				}
			}
			if paramCount > 0 {
				for {
					paramData, err := mc.ReadRawPacket()
					if err != nil {
						fmt.Println(err)
						_ = Err( conn, 2,"receive data from server error")
						continue
					}
					conn.Write(paramData)
					//serverData = append(serverData, paramData...)
					if paramData[4]==0xfe || paramData[4]==0xff{
						break
					}
				}
			}
		default:
			switch first[4] {
			case 0x00, 0xfb, 0xff: //OK,,Err
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
					//serverData = append(serverData, fieldData...)
					conn.Write(fieldData)
				}
				for {
					data, err := mc.ReadRawPacket()
					if err != nil {
						//fmt.Println(err)
						break
					}
					conn.Write(data)
					//serverData = append(serverData, data...)
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
		}

		//fmt.Println("serverData:",serverData,fieldLen)
		//fmt.Println(conn.Write(serverData))
		//重置seq
		ins.ResetSeq()

	}
}


func (ins *Instance) ResetSeq() {
	ins.seq = 0
}