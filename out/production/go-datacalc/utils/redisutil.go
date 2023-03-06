package utils

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"github.com/go-redis/redis"
)

func ConnectRedis() *redis.Client {
	fmt.Println("GetRedis() run")
	client := redis.NewClient(&redis.Options{
		Addr:        RedisAddr,
		Password:    RedisPassword, // no password set
		DB:          0,             // use default DB
		MaxConnAge:  200,
		IdleTimeout: 600,
	})
	return client
}
func SetRedis(key string, value string) {
	client := ConnectRedis()
	defer func(client *redis.Client) {
		err := client.Close()
		if err != nil {
			fmt.Println(err)
		}
	}(client)
	// 设置一个键值对
	err := client.Set(key, value, 0).Err()
	if err != nil {
		fmt.Println(err)
	}
}
func GetRedis() string {
	client := ConnectRedis()
	defer func(client *redis.Client) {
		err := client.Close()
		if err != nil {
			fmt.Println(err)
		}
	}(client)
	// 获取一个键的值
	val, err := client.Get("key").Result()
	if err != nil {
		fmt.Println(err)
	}
	return val
}

func DoRedis(arg ...any) {
	client := ConnectRedis()
	defer func(client *redis.Client) {
		err := client.Close()
		if err != nil {
			fmt.Println(err)
		}
	}(client)
	// 执行一个Redis命令
	err := client.Do(arg...).Err()
	if err != nil {
		fmt.Println(err)
	}
}
func PublishRedis() {
	MsgList := GetMsgInstance()
	message := MsgList.Msg
	client := ConnectRedis()
	defer func(client *redis.Client) {
		err := client.Close()
		if err != nil {
			fmt.Println(err)
		}
	}(client)
	var msgList string
	for i, str := range message {
		if i > 0 {
			msgList += ","
		}
		msgList += str
	}
	buf := new(bytes.Buffer)
	writer := zlib.NewWriter(buf)
	_, err := writer.Write([]byte("," + msgList))
	if err != nil {
		panic(err)
	}
	err = writer.Close()
	if err != nil {
		panic(err)
	}
	compressedData := buf.Bytes()
	err = client.Publish(RedisChannels, compressedData).Err()
	if err != nil {
		fmt.Println(err)
	}
}
