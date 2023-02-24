package utils

import (
	"fmt"
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gctx"
)

var (
	KairosdbHost  = GetConfig("kairosdb.host")
	KairosdbPort  = GetConfig("kairosdb.port")
	RedisAddr     = GetConfig("redis.Addr")
	RedisPassword = GetConfig("redis.Password")
	RedisChannels = GetConfig("redis.channels")
	MysqlUser     = GetConfig("mysql.user")
	MysqlPassword = GetConfig("mysql.password")
	MysqlAddr     = GetConfig("mysql.addr")
	MysqlDatabase = GetConfig("mysql.database")

	KairosDb = "http://" + KairosdbHost + ":" + KairosdbPort
)

// 读取配置文件
func GetConfig(name string) string {
	ctx := gctx.New()
	config, err := g.Cfg().Get(ctx, name)
	if err != nil {
		fmt.Println("Error:", err)
		return ""
	}
	str := config.String()
	return str
}
