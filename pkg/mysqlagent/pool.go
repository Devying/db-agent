package mysqlagent

import (
	"strconv"
)
func (ins *Instance)GetPool() (InstanceConn,error){
	select {
	case p := <-ins.Pool:
		return p,nil
	default:
		var conn InstanceConn
		err := conn.Connect(ins.Conf.User+":"+ins.Conf.Pass+"@tcp("+ins.Conf.Host+":"+strconv.Itoa(ins.Conf.Port)+")/"+ins.Conf.DB)
		return conn,err
	}
}
func (ins *Instance)PutPool(conn InstanceConn) error{
	select {
	case ins.Pool <-conn:
		return nil
	default:
		return nil
	}
}