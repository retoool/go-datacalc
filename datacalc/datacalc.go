package datacalc

import (
	"fmt"
	"go-datacalc/utils"
	"go-datacalc/utils/kdb"
)

func DataCalc() {
	//s := Newsqldata()
	beginTimeStr, endTimeStr := utils.TimeInit()
	beginTime, endTime := utils.StrToTime(beginTimeStr), utils.StrToTime(endTimeStr)
	//fmt.Println(tags)

	//WNAC_WdSpd_test := kdb.KairosdbClient("WNAC_WdSpd", []string{"DTNXJK:TYSFC:Q2:W055", "DTNXJK:TYSFC:Q2:W056"}, "avg", beginTime, endTime, "end", "0", "50", "10", "minutes")
	WNAC_WdSpd_test := kdb.KairosdbClient("WNAC_WdSpd_test", []string{"DTNXJK:TYSFC:Q2:W055", "DTNXJK:TYSFC:Q2:W056"}, "avg", beginTime, endTime, "end", "", "", "10", "minutes")
	//kdb.PushKdb("WNAC_WdSpd_test", WNAC_WdSpd_test)
	fmt.Println(WNAC_WdSpd_test)

}
