package mysqlagent

import "net"

func Err(conn net.Conn,seq byte,msg string)error {
	//出现错误回复
	head := make([]byte,4)
	//1个0xff+2个错误代码
	pktLen := len(msg)+1+2
	head[0] = byte(pktLen)
	head[1] = byte(pktLen >> 8)
	head[2] = byte(pktLen >> 16)
	head[3] = seq
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