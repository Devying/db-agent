package server

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	vlog "github.com/Devying/db-agent/log"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/Devying/db-agent/config"
	"github.com/Devying/db-agent/third_party/redigo/redis"
)

type Redis struct {
	Conf map[string]config.RedisConfig
	Ins  map[string]*RedisInstance
}
type RedisInstance struct {
	Pool *redis.Pool
}

func (r *Redis) Config() error {
	r.Conf = config.ParseRedisConfig()
	return nil
}

func (r *Redis) GetIns(ins string) (*RedisInstance, error) {
	if ins, ok := r.Ins[ins]; ok {
		return ins, nil
	}
	return nil, errors.New("instance does not exist")
}
func (r *Redis) Initialize() error {
	r.Ins = make(map[string]*RedisInstance)
	for k, v := range r.Conf {
		ins := &RedisInstance{}
		ins.Pool = newPool(v.Host + ":" + strconv.Itoa(v.Port))
		r.Ins[k] = ins
	}
	return nil
}
func newPool(server string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     500,
		IdleTimeout: 2 * time.Minute,
		MaxRetries:  3,

		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			return c, err
		},
		Wait: true, //超时等待否则太多连接会报read: connection reset by peer

		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			if _, err := c.Do("PING"); err != nil {
				vlog.Errorf("[newPool] pcn ping err: %s", err.Error())
				return err
			}
			return nil
		},
	}
}
func (r *Redis) Process(conn net.Conn) {
	buf := bufio.NewReader(conn)
	var ins string
	for {
		//读取协议
		protocol, err := ReadProtocol(buf)
		if err == io.EOF {
			vlog.Infoln("[Redis Process] client exit")
			return
		}
		if err != nil {
			_, e := conn.Write(r.ErrorRes(err))
			if e != nil {
				return
			}
			continue
		}
		cmdLine := r.DecodeProtocol(protocol)

		//解析连接
		if len(cmdLine) == 1 && strings.ToUpper(cmdLine[0]) == "COMMAND" {
			_, e := conn.Write([]byte("+OK\r\n"))
			if e != nil {
				vlog.Errorf("[Redis Process] write err: %s", e.Error())
				return
			}
			continue
		}
		//解析auth协议 获取要连接的实例
		if len(cmdLine) == 2 && strings.ToUpper(cmdLine[0]) == "AUTH" {
			//protocol = r.EncodeProtocol(protocol)
			s := strings.Split(strings.ToLower(cmdLine[1]), "@")
			if len(s) < 2 {
				_, e := conn.Write(r.ErrorRes(errors.New("instance error")))
				if e != nil {
					vlog.Errorf("[Redis Process] write err: %s", e.Error())
					return
				}
				continue
			}
			ins = s[1]
			conn.Write([]byte("+OK\r\n"))
			continue
		}

		if ins == "" {
			_, e := conn.Write(r.ErrorRes(errors.New("select an instance")))
			if e != nil {
				vlog.Errorf("[Redis Process] write err: %s", e.Error())
				return
			}
			continue
		}
		ins, err := r.GetIns(ins)
		if err != nil {
			_, e := conn.Write(r.ErrorRes(err))
			if e != nil {
				return
			}
			continue
		}

		for attempt := 0; attempt < ins.Pool.MaxRetries; attempt++ {
			if attempt > 0 {
				time.Sleep(8 * time.Millisecond)
			}
			pcn := ins.Pool.Get()
			resp, err := pcn.DoProtocol(protocol)
			if pcnErr := pcn.Close(); pcnErr != nil { //释放连接到连接池
				vlog.Errorf("[Redis Process] release pcn err: %s", pcnErr.Error())
			}
			if err != nil {
				vlog.Errorf("[Redis Process] DoProtocol err: %s", err.Error())
				if pcn.IsRetryableError(err) {
					continue
				}
				_, e := conn.Write(r.ErrorRes(err))
				if e != nil {
					vlog.Errorf("[Redis Process] write err: %s", e.Error())
					return
				}
				break
			}
			_, e := conn.Write(resp)
			if e != nil {
				vlog.Errorf("[Redis Process] write err: %s", err.Error())
				return
			}
			break
		}
	}
}
func (r *Redis) DecodeProtocol(p []byte) []string {
	if len(p) == 0 {
		return []string{}
	}
	cmd := bytes.Split(p, []byte{'\r', '\n'})
	cmd = cmd[1 : len(cmd)-1]
	var cmdLine []string
	for i := 0; i < len(cmd); i += 2 {
		cmdLine = append(cmdLine, string(cmd[i+1]))
	}
	return cmdLine
}

func (r *Redis) EncodeProtocol(p []byte) []byte {
	if len(p) == 0 {
		return []byte{}
	}
	cmd := bytes.Split(p, []byte{'\r', '\n'})
	var protocol []byte
	for i := 0; i < len(cmd); i++ {
		if i == len(cmd)-2 {
			s := strings.Split(string(cmd[i]), "@")
			protocol = append(protocol, []byte(s[0])...)
		} else {
			protocol = append(protocol, cmd[i]...)
		}
		if len(cmd[i]) > 0 {
			protocol = append(protocol, []byte("\r\n")...)
		}
	}
	return protocol
}

func readProtocolLine(buf *bufio.Reader) ([]byte, error) {
	p, err := buf.ReadBytes('\n')
	if err == bufio.ErrBufferFull {
		return nil, errors.New("long request line")
	}
	if err != nil {
		return nil, err
	}
	i := len(p) - 2
	if i < 0 || p[i] != '\r' {
		return nil, errors.New("bad request line terminator")
	}
	return p, nil
}

// parseLen parses bulk string and array lengths.
func parseProtocolLen(p []byte) (int, error) {
	if len(p) == 0 {
		return -1, errors.New("malformed length")
	}

	if p[0] == '-' && len(p) == 2 && p[1] == '1' {
		// handle $-1 and $-1 null replies.
		return -1, nil
	}

	var n int
	for _, b := range p {
		if b == '\r' || b == '\n' {
			continue
		}
		n *= 10
		if b < '0' || b > '9' {
			println("eeeeeeee")
			return -1, errors.New("illegal bytes in length")
		}
		n += int(b - '0')
	}

	return n, nil
}

// parseInt parses an integer reply.
func parseProtocolInt(p []byte) (int64, error) {
	if len(p) == 0 {
		return 0, errors.New("malformed integer")
	}

	var negate bool
	if p[0] == '-' {
		negate = true
		p = p[1:]
		if len(p) == 0 {
			return 0, errors.New("malformed integer")
		}
	}

	var n int64
	for _, b := range p {
		n *= 10
		if b < '0' || b > '9' {
			return 0, errors.New("illegal bytes in length")
		}
		n += int64(b - '0')
	}

	if negate {
		n = -n
	}
	return n, nil
}

func ReadProtocol(buf *bufio.Reader) ([]byte, error) {
	line, err := readProtocolLine(buf)
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, errors.New("short response line")
	}
	println(string(line))
	switch line[0] {
	case '$':
		n, err := parseProtocolLen(line[1:])
		if n < 0 || err != nil {
			return nil, err
		}
		p := make([]byte, n+2)
		_, err = io.ReadFull(buf, p)
		if err != nil {
			return nil, err
		}
		p = append(line, p...)
		return p, nil
	case '*':
		n, err := parseProtocolLen(line[1:])
		if n < 0 || err != nil {
			return nil, err
		}
		var r []byte

		for i := 0; i < n; i++ {
			tmp, err := ReadProtocol(buf)
			if err != nil {
				return nil, err
			}
			r = append(r, tmp...)
		}
		r = append(line, r...)
		return r, nil
	}
	return nil, errors.New("unexpected response line")
}
func (r *Redis) ErrorRes(err error) []byte {
	return []byte("-" + fmt.Sprintf("%s", err) + "\r\n")
}
