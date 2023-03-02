package datacalc

import (
	"fmt"
	"go-datacalc/utils"
	"go-datacalc/utils/kdb"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

func Run() {
	beginTimeStr, endTimeStr := utils.TimeInit()
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
	//response := kdb.PushMsgToKdb()
	fmt.Println("EndTime: ", time.Now())
	//fmt.Println("StatusCode: ", response.StatusCode)
	fmt.Println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
}
func GetData(devMap []string, beginTime time.Time, endTime time.Time) {
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
	ActPWR_AVG_10m := kdb.QueryKdb("ActPWR", devMap, "avg", beginTime, endTime, "end", "", "", "10", "minutes")
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
	NewCalcRT_StndSt_LAST_10m := kdb.QueryKdb("NewCalcRT_StndSt", devMap, "last", beginTime, endTime, "end", "", "", "10", "minutes")
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
	NewCalcRT_StndSt := kdb.QueryKdb("NewCalcRT_StndSt", GetSqlDataInstance().codeSlice, "sum", beginTime, endTime, "", "", "", "1", "milliseconds")
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
				fmt.Println(err)
				continue
			}
			WNAC_WdSpd_DEV_10mi, err := utils.GetCache("WNAC_WdSpd_DEV_10m", hashKey, timestamp)
			if err != nil {
				fmt.Println(err)
				continue
			}
			NewCalcRT_StndSt_AVG_10mi, err := utils.GetCache("NewCalcRT_StndSt_AVG_10m", hashKey, timestamp)
			if err != nil {
				fmt.Println(err)
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
				fmt.Println(err)
				continue
			}
			if NewCalcRT_StndSt_AVG_10mi != 5 {
				utils.SetCache("WNAC_WdSpd_MAX_10m", hashKey, timestamp, value, true)
			}
		}
	}
}
func PwrCalc(devMap []string, beginTime time.Time, endTime time.Time) {
	s := GetSqlDataInstance()
	frequency := 60 * 10
	timeList := utils.SplitTimeList(beginTime, endTime, frequency)
	powerCurveHisMap, err := GetPowerCurveHis()
	if err != nil {
		fmt.Println(err)
	}
	for _, timestr := range timeList {
		timeT, err := time.Parse("2006-01-02 15:04:05", timestr)
		timestamp := int(timeT.UnixMilli())
		if err != nil {
			fmt.Println(err)
		}
		for _, HashKey := range devMap {
			WNAC_WdSpd_AVG_10m, err := utils.GetCache("WNAC_WdSpd_AVG_10m", HashKey, timestamp)
			if err != nil {
				break
			}
			NewCalcRT_StndSt_AVG_10m, err := utils.GetCache("NewCalcRT_StndSt_AVG_10m", HashKey, timestamp)
			if err != nil {
				break
			}
			WNAC_ExTmp_AVG_10m, err := utils.GetCache("WNAC_ExTmp_AVG_10m", HashKey, timestamp)
			if err != nil {
				break
			}
			ActPWR_AVG_10m, err := utils.GetCache("ActPWR_AVG_10m", HashKey, timestamp)
			if err != nil {
				break
			}
			hashKeySplits := strings.Split(HashKey, ":")
			project := hashKeySplits[0]
			farm := hashKeySplits[1]
			term := hashKeySplits[2]
			term_full := strings.Join([]string{project, farm, term}, ":")
			farm_full := strings.Join([]string{project, farm}, ":")
			windType := s.devMap[HashKey].machineTypeCode
			windSpeedCutInStr := s.typeMap[windType].windSpeedCutIn
			windSpeedCutIn, err := utils.StrToFloat(windSpeedCutInStr)
			if err != nil {
				fmt.Println(HashKey + "切入风速为空")
			}
			windSpeedCutOutStr := s.typeMap[windType].windSpeedCutOut
			windSpeedCutOut, err := utils.StrToFloat(windSpeedCutOutStr)
			if err != nil {
				fmt.Println(HashKey + "切出风速为空")
			}
			capacityStr := s.typeMap[windType].capacity
			capacity, err := utils.StrToFloat(capacityStr)
			if err != nil {
				fmt.Println(HashKey + "装机容量为空")
			}
			powerCurveEntity := s.typeMap[windType].powerCurve
			powerCurve := [][]float64{}
			for _, v := range powerCurveEntity {
				speed := v.speed
				power := v.power
				powerCurve = append(powerCurve, []float64{speed, power})
			}
			sort.Slice(powerCurve, func(i, j int) bool {
				return powerCurve[i][0] < powerCurve[j][0]
			})
			altitudeStr := s.devMap[HashKey].altitude
			altitude, err := utils.StrToFloat(altitudeStr)
			if err != nil {
				fmt.Println(HashKey + "海拔高度为空")
			}
			hubHeightStr := s.devMap[HashKey].hubHeight
			if err != nil {
				fmt.Println(HashKey + "轮毂高度为空")
			}
			hubHeight, err := utils.StrToFloat(hubHeightStr)
			P_10m := 101325 * math.Exp(-(altitude+hubHeight)*9.8/(287.05*(273.15+WNAC_ExTmp_AVG_10m))) // 10分钟大气压强
			Pw := 0.0000205 * math.Exp(0.0613846*(273.15+WNAC_ExTmp_AVG_10m))                          // 10分钟特定温度大气压
			density_10m := (P_10m/287.05 - 0.5*Pw*(1/287.05-1/461.5)) / (273.15 + WNAC_ExTmp_AVG_10m)  // 10分钟空气密度
			windspd_stnd := math.Pow(math.Pow(WNAC_WdSpd_AVG_10m, 3)*density_10m/1.225, 0.33333333)    // 标准空气密度风速
			Interval_array := make([]float64, 100)
			for i := 0; i < 100; i++ {
				Interval_array[i] = 0 + float64(i)*0.5
			}
			idx := utils.FindNearestIndex(Interval_array, windspd_stnd)
			WNAC_WdSpd_Interval_10m := float64(int(Interval_array[idx]*10)) / 10 // 10分钟风速区间对应值，保留一位小数
			utils.SetCache("WNAC_WdSpd_Interval_10m", HashKey, timestamp, WNAC_WdSpd_Interval_10m, true)
			density_10m = utils.Round(density_10m, 6)
			windspd_stnd = utils.Round(windspd_stnd, 6)
			utils.SetCache("CalcRT_density_AVG_10m", HashKey, timestamp, density_10m, true)
			utils.SetCache("CalcRT_WdSpdStnd_AVG_10m", HashKey, timestamp, windspd_stnd, true)
			minWindSpd := windSpeedCutIn - 1 // 区间最小风速
			maxWindSpd := 50.0               // 区间最大风速
			entertag := 0
			var Theory_PWR_Inter, Theory_PWR_Inter_his, Theory_PWR_Interval, Theory_PWR_Interval_his float64
			for i := 0; i < len(powerCurve); i++ {
				if powerCurve[i][0] > 0 {
					var prepower, prewindspd float64
					windspd := powerCurve[i][0]
					power := powerCurve[i][1]
					if i == 0 {
						prewindspd = 0.0
						prepower = 0.0
					} else {
						prewindspd = powerCurve[i-1][0]
						prepower = powerCurve[i-1][1]
					}

					if WNAC_WdSpd_Interval_10m == windspd {
						Theory_PWR_Interval = power
						utils.SetCache("Theory_PWR_Interval", HashKey, timestamp, Theory_PWR_Interval, true)
					}

					if power >= capacity*0.85 && entertag == 0 {
						entertag = 1
						if i == 0 {
							if windspd == 0 {
								maxWindSpd = 0
							} else {
								maxWindSpd = ((windspd-0)*(capacity*0.85-0))/(windspd-0) + prewindspd
							}
						} else {
							maxWindSpd = ((windspd-prewindspd)*(capacity*0.85-prepower))/(power-prepower) + prewindspd
						}
					}

					if prewindspd <= windspd_stnd && windspd_stnd <= windspd {
						theory_pwr := ((power-prepower)*(windspd_stnd-prewindspd))/(windspd-prewindspd) + prepower
						Theory_PWR_Inter = utils.Round(theory_pwr, 6)
						utils.SetCache("Theory_PWR_Inter", HashKey, timestamp, Theory_PWR_Inter, true)
					}
				}
			}
			powerCurveHis := powerCurveHisMap[HashKey]
			if powerCurveHis == nil {
				break
			}
			sort.Slice(powerCurveHis, func(i, j int) bool {
				return powerCurveHis[i][0] < powerCurveHis[j][0]
			})
			for i := 0; i < len(powerCurveHis); i++ {
				if powerCurveHis[i][0] > 0 {
					var prepower, prewindspd float64
					windspd := powerCurveHis[i][0]
					power := powerCurveHis[i][1]
					if i == 0 {
						prewindspd = 0.0
						prepower = 0.0
					} else {
						prewindspd = powerCurveHis[i-1][0]
						prepower = powerCurveHis[i-1][1]
					}
					if WNAC_WdSpd_Interval_10m == windspd {
						Theory_PWR_Interval_his = power
						utils.SetCache("Theory_PWR_Interval_his", HashKey, timestamp, Theory_PWR_Interval_his, true)
					}
					if prewindspd <= windspd_stnd && windspd_stnd <= windspd {
						theory_pwr := ((power-prepower)*(windspd_stnd-prewindspd))/(windspd-prewindspd) + prepower
						Theory_PWR_Inter_his = utils.Round(theory_pwr, 6)
						utils.SetCache("Theory_PWR_Inter_his", HashKey, timestamp, Theory_PWR_Inter_his, true)
					}
				}
			}
			var ActPWR_Filter_Tag float64
			if NewCalcRT_StndSt_AVG_10m == 1 && ActPWR_AVG_10m < 2*capacity && ActPWR_AVG_10m > 0 {
				ActPWR_Filter_Tag = 0
			} else {
				ActPWR_Filter_Tag = 1
			}
			utils.SetCache("ActPWR_Filter_Tag", HashKey, timestamp, ActPWR_Filter_Tag, true)
			if ActPWR_Filter_Tag == 0 && windspd_stnd <= maxWindSpd && windspd_stnd >= minWindSpd {
				utils.SetCache("ActPWR_Filter_AVG_10m", HashKey, timestamp, ActPWR_AVG_10m, true)
				utils.SetCache("Theory_PWR_Inter_Filter", HashKey, timestamp, Theory_PWR_Inter, true)
				utils.SetCache("Theory_PWR_Inter_Filter_his", HashKey, timestamp, Theory_PWR_Inter_his, true)
			}
			if ActPWR_Filter_Tag == 0 && windspd_stnd <= windSpeedCutOut && windspd_stnd >= 0 {
				utils.SetCache("ActPWR_Fitting_AVG_10m", HashKey, timestamp, ActPWR_AVG_10m, true)
				utils.SetCache("Theory_PWR_Inter_Fitting", HashKey, timestamp, Theory_PWR_Inter, true)
				utils.SetCache("Theory_PWR_Inter_Fitting_his", HashKey, timestamp, Theory_PWR_Inter_his, true)
			}
			SumList_10m := []string{
				"ActPWR_Filter_AVG_10m",
				"Theory_PWR_Inter",
				"Theory_PWR_Inter_his",
				"Theory_PWR_Inter_Filter",
				"Theory_PWR_Inter_Filter_his",
			}
			AvgList_10m := []string{
				"CalcRT_density_AVG_10m",
				"WNAC_WdSpd_FilterAVG_10m",
				"WNAC_WdSpd_AVG_10m",
			}
			MaxList_10m := []string{
				"WNAC_WdSpd_MAX_10m",
			}
			SumMap_10m := make(map[string][]float64)
			for _, point := range SumList_10m {
				pointvalue, err := utils.GetCache(point, HashKey, timestamp)
				if err != nil {
					continue
				}
				if SumMap_10m[term_full+"&"+point] == nil {
					SumMap_10m[term_full+"&"+point] = []float64{0, 1}
				}
				if SumMap_10m[farm_full+"&"+point] == nil {
					SumMap_10m[farm_full+"&"+point] = []float64{0, 1}
				}
				SumMap_10m[term_full+"&"+point][0] += pointvalue
				SumMap_10m[farm_full+"&"+point][0] += pointvalue
			}
			for _, point := range AvgList_10m {
				pointvalue, err := utils.GetCache(point, HashKey, timestamp)
				if err != nil {
					continue
				}
				if SumMap_10m[term_full+"&"+point] == nil {
					SumMap_10m[term_full+"&"+point] = []float64{0, 0}
				}
				if SumMap_10m[farm_full+"&"+point] == nil {
					SumMap_10m[farm_full+"&"+point] = []float64{0, 0}
				}
				SumMap_10m[term_full+"&"+point][0] += pointvalue
				SumMap_10m[farm_full+"&"+point][0] += pointvalue
				SumMap_10m[term_full+"&"+point][1] += 1
				SumMap_10m[farm_full+"&"+point][1] += 1
			}
			for _, point := range MaxList_10m {
				pointvalue, err := utils.GetCache(point, HashKey, timestamp)
				if err != nil {
					continue
				}
				if SumMap_10m[term_full+"&"+point] == nil {
					SumMap_10m[term_full+"&"+point] = []float64{pointvalue, 1}
				}
				if SumMap_10m[farm_full+"&"+point] == nil {
					SumMap_10m[farm_full+"&"+point] = []float64{pointvalue, 1}
				}
				SumMap_10m[term_full+"&"+point][0] = math.Max(SumMap_10m[term_full+"&"+point][0], pointvalue)
				SumMap_10m[farm_full+"&"+point][0] = math.Max(SumMap_10m[farm_full+"&"+point][0], pointvalue)
			}
			for k, v := range SumMap_10m {
				split := strings.Split(k, "&")
				hashkey := split[0]
				point := split[1]
				value := utils.Round(v[0]/v[1], 6)
				utils.SetCache(point, hashkey, timestamp, value, true)
			}
		}
	}
}
