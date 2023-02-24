package utils

import (
	"strconv"
	"sync"
)

type MsgList struct {
	Msg []string
}

var instance *MsgList
var once sync.Once

func GetMsgInstance() *MsgList {
	once.Do(func() {
		instance = &MsgList{}
	})
	return instance
}
func SetMsgMap(paraName string, dataMap map[string][][]string) {
	msgList := GetMsgInstance()
	for devName := range dataMap {
		for i := 0; i < len(dataMap[devName]); i++ {
			time := dataMap[devName][i][0]
			value := dataMap[devName][i][1]
			msgList.Msg = append(msgList.Msg, devName+":"+paraName+"@F:"+value+":"+time)
		}
	}
}

func SetMsgOne(paraName string, devName string, time int, value float64) {
	timestr := strconv.Itoa(time)
	valuestr := strconv.FormatFloat(value, 'f', -1, 64)
	msgList := GetMsgInstance()
	msgList.Msg = append(msgList.Msg, devName+":"+paraName+"@F:"+valuestr+":"+timestr)
}
