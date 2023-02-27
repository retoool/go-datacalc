package utils

import (
	"errors"
	"sync"
)

type DataCalc struct {
	CacheData map[string]map[string]map[int]float64
}

var instanceCache *DataCalc
var onceCache sync.Once

func GetCacheInstance() *DataCalc {
	onceCache.Do(func() {
		instanceCache = &DataCalc{}
	})
	return instanceCache
}
func SetCache(pointName string, hashKey string, timestamp int, value float64, setMsgList bool) {
	d := GetCacheInstance()
	if d.CacheData == nil {
		d.CacheData = make(map[string]map[string]map[int]float64)
	}
	if d.CacheData[pointName] == nil {
		d.CacheData[pointName] = make(map[string]map[int]float64)
	}
	if d.CacheData[pointName][hashKey] == nil {
		d.CacheData[pointName][hashKey] = make(map[int]float64)
	}
	d.CacheData[pointName][hashKey][timestamp] = value
	if setMsgList == true {
		SetMsgOne(pointName, hashKey, timestamp, value)
	}
}

func GetCache(pointName string, hashKey string, timestamp int) (float64, error) {
	d := GetCacheInstance()
	if d.CacheData == nil {
		return 0, errors.New(pointName + ":" + hashKey + "缓存值获取失败1")
	}
	if d.CacheData[pointName] == nil {
		return 0, errors.New(pointName + ":" + hashKey + "缓存值获取失败2")
	}
	if d.CacheData[pointName][hashKey] == nil {
		return 0, errors.New(pointName + ":" + hashKey + "缓存值获取失败3")
	}
	if _, ok := d.CacheData[pointName][hashKey][timestamp]; ok {
		value := d.CacheData[pointName][hashKey][timestamp]
		return value, nil
	}
	return 0, errors.New(pointName + ":" + hashKey + "缓存值获取失败4")
}
