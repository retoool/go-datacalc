package datacalc

import (
	"fmt"
	"go-datacalc/utils"
	"go-datacalc/utils/kdb"
)

func DeleteKdbData(beginTimeStr, endTimeStr string) {
	fmt.Println("DeleteKdbData() Run")
	fmt.Println("DeleteTimeRange: " + beginTimeStr + " to " + endTimeStr)
	delMetric := []string{
		"WNAC_WdSpd_AVG_10m",
		"WNAC_WdSpd_DEV_10m",
		"WNAC_WdSpd_Interval_10m",
		"NewCalcRT_StndSt_AVG_10m",
		"WNAC_ExTmp_AVG_10m",
		"ActPWR_AVG_10m",
		"CalcRT_density_AVG_10m",
		"CalcRT_WdSpdStnd_AVG_10m",
		"ActPWR_Filter_Tag",
		"ActPWR_Filter_AVG_10m",
		"Theory_PWR_Inter",
		"Theory_PWR_Inter_Filter",
		"Theory_PWR_Interval",
		"ActPWR_Fitting_AVG_10m",
		"Theory_PWR_Inter_Fitting",
		"Theory_PWR_Inter_Fitting_his",
		"Theory_PWR_Inter_Filter_his",
		"Theory_PWR_Inter_his",
		"Theory_PWR_Inter_Filter_his",
		"WNAC_WdSpd_FilterAVG_10m",
	}
	beginTime, endTime := utils.StrToTime(beginTimeStr), utils.StrToTime(endTimeStr)
	fmt.Println(beginTime, endTime)

	for _, v := range delMetric {
		fmt.Println("DeleteMetric: " + v)
		response := kdb.DeteleMetricRange(v, beginTime, endTime)
		fmt.Println(response.StatusCode)
	}
	fmt.Println("DeleteKdbData() End")
	fmt.Println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
}
