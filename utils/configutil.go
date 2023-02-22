package utils

import (
	"github.com/gogf/gf/v2/container/gvar"
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gctx"
)

var (
	KairosdbHost  = GetConfig("kairosdb.host").String()
	KairosdbPort  = GetConfig("kairosdb.port").String()
	RedisInfoBus  = GetConfig("redis.infobus").String()
	RedisChannels = GetConfig("redis.channels").String()
	MysqlUser     = GetConfig("mysql.user").String()
	MysqlPassword = GetConfig("mysql.password").String()
	MysqlAddr     = GetConfig("mysql.addr").String()
	MysqlDatabase = GetConfig("mysql.database").String()

	KairosDb = "http://" + KairosdbHost + ":" + KairosdbPort
)

// 读取配置文件
func GetConfig(name string) *gvar.Var {
	ctx := gctx.New()
	config, err := g.Cfg().Get(ctx, name)
	if err != nil {
		panic(err.Error())
	}
	return config
}
