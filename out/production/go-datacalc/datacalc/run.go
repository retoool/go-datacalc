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
	codeMap := s.codeSlice
	GetData(codeMap, beginTime, endTime)
	fmt.Println("GetData() Done: ", time.Now())
	PwrCalc(codeMap, beginTime, endTime)
	fmt.Println("PwrCalc() Done: ", time.Now())
	CalcLostPower(beginTime, endTime)
	fmt.Println("CalcLostPower() Done: ", time.Now())
	response := kdb.PushMsgToKdb()
	fmt.Println("StatusCode: ", response.StatusCode)
	fmt.Println("EndTime: ", time.Now())
	fmt.Println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
}
func HisCalc(beginTimeStr, endTimeStr string) {
	fmt.Println(beginTimeStr + " to " + endTimeStr)
	beginTime := utils.StrToTime(beginTimeStr)
	endTime := utils.StrToTime(endTimeStr)
	frequency := 24 * 60 * 60
	timeList := utils.SplitTimeRanges(beginTime, endTime, frequency)
	for _, timerange := range timeList {
		time1str := timerange[0]
		time2str := timerange[1]
		fmt.Println(time1str + " to " + time2str)
		time1, time2 := utils.StrToTime(time1str), utils.StrToTime(time2str)
		fmt.Println("BeginTime: ", time.Now())
		s := GetSqlDataInstance()
		codeMap := s.codeSlice
		GetData(codeMap, time1, time2)
		fmt.Println("GetData() Done: ", time.Now())
		PwrCalc(codeMap, time1, time2)
		fmt.Println("PwrCalc() Done: ", time.Now())
		CalcLostPower(time1, time2)
		fmt.Println("CalcLostPower() Done: ", time.Now())
		//response := kdb.PushMsgToKdb()
		fmt.Println("EndTime: ", time.Now())
		//fmt.Println("StatusCode: ", response.StatusCode)
		fmt.Println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
	}
}
