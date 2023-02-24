package datacalc

import (
	"fmt"
	"go-datacalc/utils"
	"go-datacalc/utils/kdb"
	"math"
	"strconv"
	"strings"
	"time"
)

func Run() {
	s := GetSqlDataInstance()
	beginTimeStr, endTimeStr := utils.TimeInit()
	beginTime, endTime := utils.StrToTime(beginTimeStr), utils.StrToTime(endTimeStr)
	//GetData([]string{"DTNXJK:TXFC:Q4:W125"}, beginTime, endTime)
	PwrCalc(s, beginTime, endTime)
}
func GetData(devMap []string, beginTime time.Time, endTime time.Time) {
	WNAC_WdSpd_DEV_10m := kdb.QueryKdb("WNAC_WdSpd", devMap, "dev", beginTime, endTime, "end", "0", "50", "10", "minutes")
	for hashKey := range WNAC_WdSpd_DEV_10m {
		for i := 0; i < len(WNAC_WdSpd_DEV_10m[hashKey]); i++ {
			timestamp, err := strconv.Atoi(WNAC_WdSpd_DEV_10m[hashKey][i][0])
			if err != nil {
				fmt.Println(err)
			}
			timestamp = timestamp / 1000
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
			timestamp = timestamp / 1000
			value, err := strconv.ParseFloat(WNAC_ExTmp_AVG_10m[hashKey][i][1], 64)
			if err != nil {
				fmt.Println(err)
			}
			utils.SetCache("WNAC_ExTmp_AVG_10m", hashKey, timestamp, value, true)
		}
	}
	ActPWR_AVG_10m := kdb.QueryKdb("ActPWR", devMap, "avg", beginTime, endTime, "end", "", "", "10", "minutes")
	for hashKey := range ActPWR_AVG_10m {
		for i := 0; i < len(ActPWR_AVG_10m[hashKey]); i++ {
			timestamp, err := strconv.Atoi(ActPWR_AVG_10m[hashKey][i][0])
			if err != nil {
				fmt.Println(err)
			}
			timestamp = timestamp / 1000
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
			timestamp = timestamp / 1000
			value, err := strconv.ParseFloat(NewCalcRT_StndSt_AVG_10m[hashKey][i][1], 64)
			if err != nil {
				fmt.Println(err)
			}
			utils.SetCache("NewCalcRT_StndSt_AVG_10m", hashKey, timestamp, value, true)
		}
	}
	WNAC_WdSpd_AVG_10m := kdb.QueryKdb("WNAC_WdSpd", devMap, "avg", beginTime, endTime, "end", "0", "50", "10", "minutes")
	for hashKey := range WNAC_WdSpd_AVG_10m {
		for i := 0; i < len(WNAC_WdSpd_AVG_10m[hashKey]); i++ {
			timestamp, err := strconv.Atoi(WNAC_WdSpd_AVG_10m[hashKey][i][0])
			if err != nil {
				fmt.Println(err)
			}
			timestamp = timestamp / 1000
			value, err := strconv.ParseFloat(WNAC_WdSpd_AVG_10m[hashKey][i][1], 64)
			if err != nil {
				fmt.Println(err)
			}
			WNAC_ExTmpi, err := utils.GetCache("WNAC_ExTmp", hashKey, timestamp)
			if err != nil {
				fmt.Println(err)
				break
			}
			WNAC_WdSpd_DEV_10mi, err := utils.GetCache("WNAC_WdSpd_DEV_10m", hashKey, timestamp)
			if err != nil {
				fmt.Println(err)
				break
			}
			NewCalcRT_StndSt_AVG_10mi, err := utils.GetCache("NewCalcRT_StndSt_AVG_10m", hashKey, timestamp)
			if err != nil {
				fmt.Println(err)
				break
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
			timestamp = timestamp / 1000
			value, err := strconv.ParseFloat(WNAC_WdSpd_MAX_10m[hashKey][i][1], 64)
			if err != nil {
				fmt.Println(err)
			}
			NewCalcRT_StndSt_AVG_10mi, err := utils.GetCache("NewCalcRT_StndSt_AVG_10m", hashKey, timestamp)
			if err != nil {
				fmt.Println(err)
				break
			}
			if NewCalcRT_StndSt_AVG_10mi != 5 {
				utils.SetCache("WNAC_WdSpd_MAX_10m", hashKey, timestamp, value, true)
			}
		}
	}
}
func PwrCalc(s *SqlData, beginTime time.Time, endTime time.Time) {
	frequency := 60 * 10
	timeList := utils.SplitTimeList(beginTime, endTime, frequency)
	for _, timestr := range timeList {
		timeT, err := time.Parse("2006-01-02 15:04:05", timestr)
		timestamp := int(timeT.Unix())
		if err != nil {
			fmt.Println(err)
		}
		for _, HashKey := range s.codeSlice {
			fmt.Println(HashKey)
			WNAC_WdSpd_AVG_10m, err := utils.GetCache("WNAC_WdSpd_AVG_10m", HashKey, timestamp)
			if err != nil {
				fmt.Println(err)
				break
			}
			NewCalcRT_StndSt_AVG_10m, err := utils.GetCache("NewCalcRT_StndSt_AVG_10m", HashKey, timestamp)
			if err != nil {
				fmt.Println(err)
				break
			}
			WNAC_ExTmp_AVG_10m, err := utils.GetCache("WNAC_ExTmp_AVG_10m", HashKey, timestamp)
			if err != nil {
				fmt.Println(err)
				break
			}
			ActPWR_AVG_10m, err := utils.GetCache("ActPWR_AVG_10m", HashKey, timestamp)
			if err != nil {
				fmt.Println(err)
				break
			}
			hashKeySplits := strings.Split(HashKey, ":")
			project := hashKeySplits[0]
			farm := hashKeySplits[1]
			term := hashKeySplits[2]
			term_full := strings.Join([]string{project, farm, term}, ":")
			farm_full := strings.Join([]string{project, farm}, ":")
			windSpeedCutIn := s.typeMap[HashKey].windSpeedCutIn
			windSpeedCutOut := s.typeMap[HashKey].windSpeedCutOut
			capacity := s.typeMap[HashKey].capacity
			powerCurve := s.typeMap[HashKey].powerCurve
			altitude := s.devMap[HashKey].altitude
			hubHeight := s.devMap[HashKey].hubHeight
			P_10m := 101325 * math.Exp(-(altitude+hubHeight)*9.8/(287.05*(273.15+WNAC_ExTmp_AVG_10m))) // 10分钟大气压强
			Pw := 0.0000205 * math.Exp(0.0613846*(273.15+WNAC_ExTmp_AVG_10m))                          // 10分钟特定温度大气压
			density_10m := (P_10m/287.05 - 0.5*Pw*(1/287.05-1/461.5)) / (273.15 + WNAC_ExTmp_AVG_10m)  // 10分钟空气密度
			windspd_stnd := math.Pow(WNAC_WdSpd_AVG_10m, 3) * density_10m / 1.225                      // 标准空气密度风速
			Interval_array := make([]float64, 100)
			for i := 0; i < 100; i++ {
				Interval_array[i] = 0 + float64(i)*0.5
			}
			idx := findNearestIndex(Interval_array, windspd_stnd)
			WNAC_WdSpd_Interval_10m := float64(int(Interval_array[idx]*10)) / 10 // 10分钟风速区间对应值，保留一位小数

		}
	}
}
