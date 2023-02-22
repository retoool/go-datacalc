package utils

import (
	"fmt"
	"github.com/go-redis/redis"
)

func ConnectRedis() *redis.Client {
	fmt.Println("GetRedis() run")
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	return client

}
func SetRedis(key string, value string) {
	client := ConnectRedis()
	defer client.Close()
	// 设置一个键值对
	err := client.Set(key, value, 0).Err()
	if err != nil {
		panic(err)
	}
}
func GetRedis() string {
	client := ConnectRedis()
	defer client.Close()
	// 获取一个键的值
	val, err := client.Get("key").Result()
	if err != nil {
		panic(err)
	}
	return val
}

func DoRedis(arg ...any) {
	client := ConnectRedis()
	defer client.Close()
	// 执行一个Redis命令
	err := client.Do(arg...).Err()
	if err != nil {
		panic(err.Error())
	}
}
