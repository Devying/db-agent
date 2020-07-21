package config

import (
	"flag"
	"fmt"
	"github.com/mitchellh/mapstructure"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

var Conf *Config

type Config struct {
	Port int `yaml:"port"`
	Log string `yaml:"log"`
	DB string `yaml:"db"`
	Redis string `yaml:"redis"`
	Mysql string `yaml:"mysql"`
}
type MysqlConfig struct {
	Host string `mapstructure:"host"`
	Port int `mapstructure:"port"`
	User string `mapstructure:"user"`
	Pass string `mapstructure:"pass"`
	DB string `mapstructure:"db"`
	MaxOpenConn int `mapstructure:"max_open_conn"`
	MaxIdleConn int `mapstructure:"max_idle_conn"`
}
type RedisConfig struct {
	Host string `mapstructure:"host"`
	Port int `mapstructure:"port"`
	MaxIdleConn int `mapstructure:"max_idle_conn"`
	MaxOpenConn int `mapstructure:"max_open_conn"`
}

var confFile = flag.String("conf", "", "")

func ParseConf() *Config{
	flag.Parse()
	if *confFile == "" {
		panic("confFile empty")
	}
	conf, err := ioutil.ReadFile(*confFile)
	if err != nil {
		panic(err)
	}

	var s Config
	err = yaml.Unmarshal(conf, &s)
	if err != nil {
		panic(err)
	}
	if s.DB != "redis" && s.DB != "mysql" {
		panic("Unsupported database,only redis or mysql")
	}
	return &s
}
func ParseMysqlConfig() map[string]MysqlConfig{
	conf, err := ioutil.ReadFile(Conf.Mysql)
	if err != nil {
		panic(err)
	}

	m := make(map[string]interface{})
	if err := yaml.Unmarshal(conf, m); err != nil {
		panic(err)
	}

	var mysqlConfig map[string]MysqlConfig
	err = mapstructure.Decode(m,&mysqlConfig)
	if err != nil {
		panic(err)
	}
	fmt.Println(mysqlConfig)
	return mysqlConfig
}

func ParseRedisConfig() map[string]RedisConfig{
	conf, err := ioutil.ReadFile(Conf.Redis)
	if err != nil {
		panic(err)
	}
	m := make(map[string]interface{})
	if err := yaml.Unmarshal(conf, &m); err != nil {
		panic(err)
	}
	var redisConfig map[string]RedisConfig
	err = mapstructure.Decode(m,&redisConfig)
	if err != nil {
		panic(err)
	}
	return redisConfig
}
