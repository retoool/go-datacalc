package datacalc

import (
	"go-datacalc/utils"
)

func Datacalc() {
	var s Sqldata
	s.Newsqldata()
	//for _, value := range sd.typeMap {
	//	v := fmt.Sprintf("%v", value)
	//	fmt.Println(v)
	//}
	beginTimeStr, endTimeStr := utils.TimeInit()
	beginTime := utils.StrToTime(beginTimeStr)
	endTime := utils.StrToTime(endTimeStr)
	tags := []string{"DTNXJK:HSBFC:Q2:W137"}
	utils.KairosdbClient("WNAC_WdSpd", tags, "avg",
		beginTime, endTime, "end", "", "", "", "10", "minutes")

}
