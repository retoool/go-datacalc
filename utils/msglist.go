package utils

import (
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

func SetMsgOne(paraName string, devName string, time int, value float64) {
	timestr := IntToStr(time)
	valuestr := FloatToStr(value, 6)
	msgList := GetMsgInstance()
	msgList.Msg = append(msgList.Msg, devName+":"+paraName+"@F:"+valuestr+":"+timestr)
}
