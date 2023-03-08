package datacalc

import (
	"fmt"
	"go-datacalc/utils"
	"go-datacalc/utils/kdb"
	"time"
)

func Run() {
	beginTimeStr, endTimeStr := utils.TimeInit()
	fmt.Println(beginTimeStr + " to " + endTimeStr)
	beginTime, endTime := utils.StrToTime(beginTimeStr), utils.StrToTime(endTimeStr)
	fmt.Println("BeginTime: ", time.Now())
	s := GetSqlDataInstance()
	codeMap := s.CodeSlice
	GetData(codeMap, beginTime, endTime)
	fmt.Println("GetData() Done: ", time.Now())
	PwrCalc(codeMap, beginTime, endTime)
	fmt.Println("PwrCalc() Done: ", time.Now())
	CalcLostPower(beginTime, endTime)
	fmt.Println("CalcLostPower() Done: ", time.Now())
	response := kdb.PushMsgToKdb()
	fmt.Println("PushMsgToKdb() Done: ", time.Now())
	fmt.Println("StatusCode: ", response.StatusCode)
	fmt.Println("EndTime: ", time.Now())
	fmt.Println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
}
func HisCalc(beginTimeStr, endTimeStr string) {
	fmt.Println(beginTimeStr + " to " + endTimeStr)
	time1, time2 := utils.StrToTime(beginTimeStr), utils.StrToTime(endTimeStr)
	fmt.Println("BeginTime: ", time.Now())
	s := GetSqlDataInstance()
	codeMap := s.CodeSlice
	GetData(codeMap, time1, time2)
	fmt.Println("GetData() Done: ", time.Now())
	PwrCalc(codeMap, time1, time2)
	fmt.Println("PwrCalc() Done: ", time.Now())
	CalcLostPower(time1, time2)
	fmt.Println("CalcLostPower() Done: ", time.Now())
	response := kdb.PushMsgToKdb()
	fmt.Println("StatusCode: ", response.StatusCode)
	fmt.Println("EndTime: ", time.Now())
	fmt.Println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
}
