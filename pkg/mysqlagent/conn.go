package mysqlagent

import (
	"github.com/Devying/db-agent/third_party/mysql"
)

type InstanceConn struct {
	md  mysql.MySQLDriver
}
func (i *InstanceConn) Connect(dsn string) error {
	dv := mysql.MySQLDriver{}
	_, err := dv.Open(dsn)
	if err != nil {
		return err
	}
	i.md = dv
	return nil
}

