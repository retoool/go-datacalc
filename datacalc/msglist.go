package datacalc

import "sync"

type MsgList struct {
	Msg []string
}

var instance *MsgList
var once sync.Once

func GetInstance() *MsgList {
	once.Do(func() {
		instance = &MsgList{}
	})
	return instance
}
func SetMsgList(devName, paraName, dataType, value, time string) {
	msgList := GetInstance()
	msgList.Msg = append(msgList.Msg, devName+":"+paraName+"@"+dataType+":"+value+":"+time)
}
func PullMsgList(*map[string]map[int64]float64) {

}
