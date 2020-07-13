package config

import (
	"fmt"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
)
type Config struct {
	Port int `yaml:"port"`
	Log string `yaml:"log"`
	DB string `yaml:"db"`
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
func ParseConf() *Config{
	viper.SetConfigName("env")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("$HOME/Project/db-agent/conf")
	err := viper.ReadInConfig()
	if err != nil {
		panic(err)
	}
	var s Config
	err = viper.Unmarshal(&s)
	if err != nil {
		panic(err)
	}
	if s.DB != "redis" && s.DB != "mysql" {
		panic("Unsupported database,only redis or mysql")
	}
	fmt.Println(s)
	return &s
}
func ParseMysqlConfig() map[string]MysqlConfig{
	viper.SetConfigName("db")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("$HOME/Project/db-agent/conf")
	err := viper.ReadInConfig()
	if err != nil {
		panic(err)
	}
	conf := viper.Get("mysql")
	var mysqlConfig map[string]MysqlConfig
	err = mapstructure.Decode(conf,&mysqlConfig)
	if err != nil {
		panic(err)
	}
	fmt.Println(mysqlConfig)
	return mysqlConfig
}

func ParseRedisConfig() map[string]RedisConfig{
	viper.SetConfigName("db")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("$HOME/Project/db-agent/conf")
	err := viper.ReadInConfig()
	if err != nil {
		panic(err)
	}
	var config map[string]interface{}
	err = viper.Unmarshal(&config)
	if err != nil {
		panic(err)
	}
	if _,ok := config["redis"];!ok{
		panic("database config is empty")
	}
	var redisConfig map[string]RedisConfig
	err = mapstructure.Decode(config["mysql"],&redisConfig)
	if err != nil {
		panic(err)
	}
	return redisConfig
}
