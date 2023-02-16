package utils

import (
	"github.com/gogf/gf/v2/container/gvar"
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gctx"
)

var (
	KairosdbHost, _  = GetConfig("kairosdb.host")
	KairosdbPort, _  = GetConfig("kairosdb.port")
	RedisInfoBus, _  = GetConfig("redis.infobus")
	RedisChannels, _ = GetConfig("redis.channels")
	MysqlLink, _     = GetConfig("database.default.link")
	MysqlCharset, _  = GetConfig("database.default.charset")

	KairosDb = "http://" + KairosdbHost.String() + ":" + KairosdbPort.String()
)

// 读取配置文件
func GetConfig(name string) (*gvar.Var, error) {
	ctx := gctx.New()
	config, err := g.Cfg().Get(ctx, name)
	if err != nil {
		return nil, err
	}
	return config, err
}
