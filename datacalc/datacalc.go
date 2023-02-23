package datacalc

import (
	"fmt"
	"go-datacalc/utils"
)

func Datacalc() {
	s := Newsqldata()
	beginTimeStr, endTimeStr := utils.TimeInit()
	beginTime, endTime := utils.StrToTime(beginTimeStr), utils.StrToTime(endTimeStr)
	tags := s.fullcodeMap
	WNAC_WdSpd_AVG_10m 		  := utils.KairosdbClient("WNAC_WdSpd", tags, "avg", beginTime, endTime, "end", "0", "50", "10", "minutes")
	WNAC_WdSpd_MAX_10m 		  := utils.KairosdbClient("WNAC_WdSpd", tags, "max", beginTime, endTime, "end", "0", "50", "10", "minutes")
	WNAC_WdSpd_DEV_10m 		  := utils.KairosdbClient("WNAC_WdSpd", tags, "dev", beginTime, endTime, "end", "0", "50", "10", "minutes")
	WNAC_ExTmp_AVG_10m 		  := utils.KairosdbClient("WNAC_ExTmp", tags, "avg", beginTime, endTime, "end", "-60", "60", "10", "minutes")
	ActPWR_AVG_10m 			  := utils.KairosdbClient("ActPWR", tags, "avg", beginTime, endTime, "end", "", "", "10", "minutes")
	NewCalcRT_StndSt_AVG_10m  := utils.KairosdbClient("NewCalcRT_StndSt", tags, "avg", beginTime, endTime, "end", "", "", "10", "minutes")




}
