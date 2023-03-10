package datacalc

import (
	"fmt"
	"go-datacalc/utils"
	"go-datacalc/utils/kdb"
	"strconv"
	"time"
)

func GetData(devMap []string, beginTime time.Time, endTime time.Time) {
	endTime = endTime.Add(-time.Second)
	WNAC_WdSpd_DEV_10m := kdb.QueryKdb("WNAC_WdSpd", devMap, "dev", beginTime, endTime, "end", "0", "50", "10", "minutes")
	for hashKey := range WNAC_WdSpd_DEV_10m {
		for i := 0; i < len(WNAC_WdSpd_DEV_10m[hashKey]); i++ {
			timestamp, err := strconv.Atoi(WNAC_WdSpd_DEV_10m[hashKey][i][0])
			if err != nil {
				fmt.Println(err)
			}
			value, err := strconv.ParseFloat(WNAC_WdSpd_DEV_10m[hashKey][i][1], 64)
			if err != nil {
				fmt.Println(err)
			}
			utils.SetCache("WNAC_WdSpd_DEV_10m", hashKey, timestamp, value, true)
		}
	}
	WNAC_ExTmp_AVG_10m := kdb.QueryKdb("WNAC_ExTmp", devMap, "avg", beginTime, endTime, "end", "-60", "60", "10", "minutes")
	for hashKey := range WNAC_ExTmp_AVG_10m {
		for i := 0; i < len(WNAC_ExTmp_AVG_10m[hashKey]); i++ {
			timestamp, err := strconv.Atoi(WNAC_ExTmp_AVG_10m[hashKey][i][0])
			if err != nil {
				fmt.Println(err)
			}
			value, err := strconv.ParseFloat(WNAC_ExTmp_AVG_10m[hashKey][i][1], 64)
			if err != nil {
				fmt.Println(err)
			}
			utils.SetCache("WNAC_ExTmp_AVG_10m", hashKey, timestamp, value, true)
		}
	}
	ActPWR_AVG_10m := kdb.QueryKdb("ActPWR", devMap, "avg", beginTime, endTime, "end", "", "100000", "10", "minutes")
	for hashKey := range ActPWR_AVG_10m {
		for i := 0; i < len(ActPWR_AVG_10m[hashKey]); i++ {
			timestamp, err := strconv.Atoi(ActPWR_AVG_10m[hashKey][i][0])
			if err != nil {
				fmt.Println(err)
			}
			value, err := strconv.ParseFloat(ActPWR_AVG_10m[hashKey][i][1], 64)
			if err != nil {
				fmt.Println(err)
			}
			utils.SetCache("ActPWR_AVG_10m", hashKey, timestamp, value, true)
		}
	}
	NewCalcRT_StndSt_AVG_10m := kdb.QueryKdb("NewCalcRT_StndSt", devMap, "avg", beginTime, endTime, "end", "", "", "10", "minutes")
	for hashKey := range NewCalcRT_StndSt_AVG_10m {
		for i := 0; i < len(NewCalcRT_StndSt_AVG_10m[hashKey]); i++ {
			timestamp, err := strconv.Atoi(NewCalcRT_StndSt_AVG_10m[hashKey][i][0])
			if err != nil {
				fmt.Println(err)
			}
			value, err := strconv.ParseFloat(NewCalcRT_StndSt_AVG_10m[hashKey][i][1], 64)
			if err != nil {
				fmt.Println(err)
			}
			utils.SetCache("NewCalcRT_StndSt_AVG_10m", hashKey, timestamp, value, true)
		}
	}
	NewCalcRT_StndSt_LAST_10m := kdb.QueryKdb("NewCalcRT_StndSt", devMap, "last", beginTime.Add(-10*time.Minute), endTime, "end", "", "", "10", "minutes")
	for hashKey := range NewCalcRT_StndSt_LAST_10m {
		for i := 0; i < len(NewCalcRT_StndSt_LAST_10m[hashKey]); i++ {
			timestamp, err := strconv.Atoi(NewCalcRT_StndSt_LAST_10m[hashKey][i][0])
			if err != nil {
				fmt.Println(err)
			}
			value, err := strconv.ParseFloat(NewCalcRT_StndSt_LAST_10m[hashKey][i][1], 64)
			if err != nil {
				fmt.Println(err)
			}
			utils.SetCache("NewCalcRT_StndSt_LAST_10m", hashKey, timestamp, value, false)
		}
	}
	NewCalcRT_StndSt := kdb.QueryKdb("NewCalcRT_StndSt", GetSqlDataInstance().CodeSlice, "sum", beginTime, endTime, "", "", "", "1", "milliseconds")
	for hashKey := range NewCalcRT_StndSt {
		for i := 0; i < len(NewCalcRT_StndSt[hashKey]); i++ {
			timestamp, err := strconv.Atoi(NewCalcRT_StndSt[hashKey][i][0])
			if err != nil {
				fmt.Println(err)
			}
			value, err := strconv.ParseFloat(NewCalcRT_StndSt[hashKey][i][1], 64)
			if err != nil {
				fmt.Println(err)
			}
			utils.SetCache("NewCalcRT_StndSt", hashKey, timestamp, value, false)
		}
	}
	WNAC_WdSpd_AVG_10m := kdb.QueryKdb("WNAC_WdSpd", devMap, "avg", beginTime, endTime, "end", "0", "50", "10", "minutes")
	for hashKey := range WNAC_WdSpd_AVG_10m {
		for i := 0; i < len(WNAC_WdSpd_AVG_10m[hashKey]); i++ {
			timestamp, err := strconv.Atoi(WNAC_WdSpd_AVG_10m[hashKey][i][0])
			if err != nil {
				fmt.Println(err)
				continue
			}
			value, err := strconv.ParseFloat(WNAC_WdSpd_AVG_10m[hashKey][i][1], 64)
			if err != nil {
				fmt.Println(err)
				continue
			}
			WNAC_ExTmpi, err := utils.GetCache("WNAC_ExTmp_AVG_10m", hashKey, timestamp)
			if err != nil {
				//fmt.Println(err)
				continue
			}
			WNAC_WdSpd_DEV_10mi, err := utils.GetCache("WNAC_WdSpd_DEV_10m", hashKey, timestamp)
			if err != nil {
				//fmt.Println(err)
				continue
			}
			NewCalcRT_StndSt_AVG_10mi, err := utils.GetCache("NewCalcRT_StndSt_AVG_10m", hashKey, timestamp)
			if err != nil {
				//fmt.Println(err)
				continue
			}
			if WNAC_WdSpd_DEV_10mi >= 0.001 || WNAC_ExTmpi >= 6 {
				utils.SetCache("WNAC_WdSpd_AVG_10m", hashKey, timestamp, value, true)
				if NewCalcRT_StndSt_AVG_10mi != 5 {
					utils.SetCache("WNAC_WdSpd_FilterAVG_10m", hashKey, timestamp, value, true)
				}
			}
		}
	}
	WNAC_WdSpd_MAX_10m := kdb.QueryKdb("WNAC_WdSpd", devMap, "max", beginTime, endTime, "end", "0", "50", "10", "minutes")
	for hashKey := range WNAC_WdSpd_MAX_10m {
		for i := 0; i < len(WNAC_WdSpd_MAX_10m[hashKey]); i++ {
			timestamp, err := strconv.Atoi(WNAC_WdSpd_MAX_10m[hashKey][i][0])
			if err != nil {
				fmt.Println(err)
			}
			timestamp = timestamp
			value, err := strconv.ParseFloat(WNAC_WdSpd_MAX_10m[hashKey][i][1], 64)
			if err != nil {
				fmt.Println(err)
			}
			NewCalcRT_StndSt_AVG_10mi, err := utils.GetCache("NewCalcRT_StndSt_AVG_10m", hashKey, timestamp)
			if err != nil {
				//fmt.Println(err)
				continue
			}
			if NewCalcRT_StndSt_AVG_10mi != 5 {
				utils.SetCache("WNAC_WdSpd_MAX_10m", hashKey, timestamp, value, true)
			}
		}
	}
}
