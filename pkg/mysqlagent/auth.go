package mysqlagent

import "net"

func (ins *Instance)AuthOK(conn net.Conn)error {
	//认证成功是第三部所以这里的包序号继续+1
	authOK := []byte{7, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0}
	_,err := conn.Write(authOK)
	return err
}