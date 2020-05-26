package config

import (
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
)

type Config struct {
	Server Server
	Databases map[string]DBConf
}

type Server struct {
	Port int
	Log string
	DB string
}
type DBConf struct {
	Host string
	Port int
	User string
	Pass string
}
var Conf = new(Config)
func ParseConf() {
	viper.SetConfigName("env")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("$HOME/Project/github.com/Devying/db-agent/conf")
	err := viper.ReadInConfig()
	if err != nil {
		panic(err)
	}
	var s Server
	err = viper.Unmarshal(&s)
	if err != nil {
		panic(err)
	}
	if s.DB != "redis" && s.DB != "mysql" {
		panic("Unsupported database,Only redis or mysql")
	}
	viper.SetConfigName("db")
	viper.SetConfigType("yaml")
	err = viper.ReadInConfig()
	if err != nil {
		panic(err)
	}
	var dbs map[string]DBConf
	dbConfig := viper.GetStringMap(s.DB)
	err = mapstructure.Decode(dbConfig,&dbs)
	if err != nil {
		panic(err)
	}
	if len(dbs)==0 {
		panic("DataBase config is empty")
	}
	Conf.Server = s
	Conf.Databases = dbs
}
