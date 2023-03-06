package datacalc

import (
	"database/sql"
	"fmt"
	"go-datacalc/utils"
	"math"
	"sort"
	"strings"
	"time"
)

// CalcLostPower 损失电量计算
func CalcLostPower(beginTime, endTime time.Time) {
	beginTimeStr := utils.TimeToStr(beginTime)
	endTimeStr := utils.TimeToStr(endTime)
	frequency := 10 * 60
	timeRanges := utils.SplitTimeRanges(beginTime, endTime, frequency)
	listings, err := GetListingData(beginTimeStr, endTimeStr)
	if err != nil {
		fmt.Println(err)
		return
	}
	mergeMap := make(map[string][][]any)
	for _, timeArr := range timeRanges {
		fromTime := int(utils.StrToTime(timeArr[0]).UnixMilli())
		toTime := int(utils.StrToTime(timeArr[1]).UnixMilli())
		lostPwrSumMap := make(map[string]map[string]float64)
		for _, HashKey := range GetSqlDataInstance().codeSlice {
			publishChList := strings.Split(HashKey, ":")
			project := publishChList[0]
			farm := publishChList[1]
			term := publishChList[2]
			termFull := strings.Join([]string{project, farm, term}, ":")
			farmFull := strings.Join([]string{project, farm}, ":")
			if lostPwrSumMap[termFull] == nil {
				lostPwrSumMap[termFull] = make(map[string]float64)
			}
			if lostPwrSumMap[farmFull] == nil {
				lostPwrSumMap[farmFull] = make(map[string]float64)
			}
			ActPWR_AVG_10m, err := utils.GetCache("ActPWR_AVG_10m", HashKey, toTime)
			if err != nil {
				//fmt.Println(err)
				continue
			}
			Theory_PWR_Inter_his, err := utils.GetCache("Theory_PWR_Inter_his", HashKey, toTime)
			if err != nil {
				//fmt.Println(err)
				continue
			}
			var lostPwr float64
			if Theory_PWR_Inter_his > ActPWR_AVG_10m {
				lostPwr = utils.Round((Theory_PWR_Inter_his-ActPWR_AVG_10m)/6, 6)
			}
			utils.SetCache("CalcRT_LostPwr_All", HashKey, toTime, lostPwr/6, true)
			if _, ok := lostPwrSumMap[termFull]["All"]; ok {
				lostPwrSumMap[termFull]["All"] += lostPwr
			} else {
				lostPwrSumMap[termFull]["All"] = lostPwr
			}
			if _, ok := lostPwrSumMap[farmFull]["All"]; ok {
				lostPwrSumMap[farmFull]["All"] += lostPwr
			} else {
				lostPwrSumMap[farmFull]["All"] = lostPwr
			}
			cI := utils.GetCacheInstance()
			if _, ok := cI.CacheData["NewCalcRT_StndSt"][HashKey]; !ok {
				continue
			}
			var stSlice [][]int
			for timestamp := range cI.CacheData["NewCalcRT_StndSt"][HashKey] {
				if timestamp >= fromTime && timestamp <= toTime {
					value := int(cI.CacheData["NewCalcRT_StndSt"][HashKey][timestamp])
					code := transFmt(value, "st")
					stSlice = append(stSlice, []int{timestamp, code, 0, 0})
				}
			}
			sort.Slice(stSlice, func(i, j int) bool {
				return stSlice[i][0] < stSlice[j][0]
			})
			if stSlice[0][0] > fromTime {
				NewCalcRT_StndSt_LAST_10mi, err := utils.GetCache("NewCalcRT_StndSt_LAST_10m", HashKey, fromTime)
				if err != nil {
					fmt.Println(err)
					continue
				}
				stSlice = append(stSlice, []int{fromTime, transFmt(int(NewCalcRT_StndSt_LAST_10mi), "st"), 0, 0})
			}
			if stSlice[len(stSlice)-1][0] < toTime {
				stSlice = append(stSlice, []int{toTime, stSlice[len(stSlice)-1][1], 0, 0})
			}
			sort.Slice(stSlice, func(i, j int) bool {
				return stSlice[i][0] < stSlice[j][0]
			})
			var listing [][]int
			if _, ok := listings[HashKey]; ok {
				listing = listings[HashKey]
			}

			listingSlice := findOverLap(listing, fromTime, toTime)
			mergeSlice := mergeTimeRange(stSlice, listingSlice)
			lostPwrMap := make(map[string]float64)
			for i := 0; i < len(mergeSlice)-1; i++ {
				timei := mergeSlice[i+1][0] - mergeSlice[i][0]
				codei := mergeSlice[i][1]
				codestr := utils.IntToStr(codei)
				lostPwrf := float64(timei) / 600000.0 * lostPwr
				lostpwri := utils.Round(lostPwrf, 6)
				mergeMap[HashKey] = append(mergeMap[HashKey], []any{
					mergeSlice[i][0],   //开始时间int
					mergeSlice[i+1][0], //结束时间int
					mergeSlice[i][1],   //损失原因int
					mergeSlice[i][2],   //挂牌信息int
					lostpwri,           //损失电量float64
					mergeSlice[i][3],   //挂牌编码int
				})
				if _, ok := lostPwrMap[codestr]; ok {
					lostPwrMap[codestr] += lostpwri
				} else {
					lostPwrMap[codestr] = lostpwri
				}

				if _, ok := lostPwrSumMap[termFull][codestr]; ok {
					lostPwrSumMap[termFull][codestr] += lostpwri
				} else {
					lostPwrSumMap[termFull][codestr] = lostpwri
				}

				if _, ok := lostPwrSumMap[farmFull][codestr]; ok {
					lostPwrSumMap[farmFull][codestr] += lostpwri
				} else {
					lostPwrSumMap[farmFull][codestr] = lostpwri
				}
			}
			for i := 1; i <= 12; i++ {
				stri := utils.IntToStr(i)
				utils.SetCache("CalcRT_LostPwr_"+stri, HashKey, toTime, 0, true)
			}
			for k, v := range lostPwrMap {
				utils.SetCache("CalcRT_LostPwr_"+k, HashKey, toTime, v, true)
			}
		}
		for k := range lostPwrSumMap {
			for i := 1; i <= 12; i++ {
				stri := utils.IntToStr(i)
				utils.SetCache("CalcRT_LostPwr_"+stri, k, toTime, 0, true)
			}
			for num, v := range lostPwrSumMap[k] {
				utils.SetCache("CalcRT_LostPwr_"+num, k, toTime, v, true)
			}
		}
	}
	resultMaps := mergeAll(mergeMap)
	var updateStatements []string
	var insertStmts []string
	sql1map := make(map[string][]string)
	sql1 := "SELECT id, machine_code, ssyy_code, ssdl, listing_code, MAX(update_date) FROM `scada_wind_power_lost` where update_date < ? group by machine_id"
	rows, err := utils.QueryMysql(sql1, beginTimeStr)
	if err != nil {
		fmt.Println(err, "select1")
	}
	var id sql.NullString
	var machineCode sql.NullString
	var ssyyCode sql.NullString
	var ssdl sql.NullString
	var listingCode sql.NullString
	var updateDate sql.NullString
	for rows.Next() {
		err := rows.Scan(&id, &machineCode, &ssyyCode, &ssdl, &listingCode, &updateDate)
		if err != nil {
			fmt.Println(err, "select2")
			continue
		}
		sql1map[machineCode.String] = []string{
			id.String,
			ssyyCode.String,
			ssdl.String,
			listingCode.String,
		}
	}
	var keylist []string
	for HashKey := range resultMaps {
		keylist = append(keylist, HashKey)
	}
	sort.Strings(keylist)
	for _, HashKey := range keylist {
		devId := GetSqlDataInstance().devMap[HashKey].id
		var keyid string
		var ssdlf float64
		var ssyyCodeInt int
		var listingCodeInt int
		if _, ok := sql1map[devId]; ok {
			smd := sql1map[devId]
			keyid = smd[0]
			ssdlf, _ = utils.StrToFloat(smd[2])
			ssyyCodeInt, _ = utils.StrToInt(smd[1])
			listingCodeInt, _ = utils.StrToInt(smd[3])
		}
		nowStr := utils.TimetoStrD(time.Now())
		if _, ok := resultMaps[HashKey]; !ok {
			continue
		}
		for i := 0; i < len(resultMaps[HashKey]); i++ {
			begintimei := resultMaps[HashKey][i][0].(int)
			endTimei := resultMaps[HashKey][i][1].(int)
			code := resultMaps[HashKey][i][2].(int)
			lostPwr := resultMaps[HashKey][i][4].(float64)
			listingcode := resultMaps[HashKey][i][5].(int)
			begintimeiStr := utils.TimeToStr(time.UnixMilli(int64(begintimei)))
			endTimeiStr := utils.TimeToStr(time.UnixMilli(int64(endTimei)))
			if i == 0 && ssyyCodeInt == code && listingCodeInt == listingcode && keyid != "" {
				sql2 := fmt.Sprintf("UPDATE `scada_wind_power_lost` SET end_date = '%s', ssdl = %f, update_date = '%s' WHERE id = %d", begintimeiStr, ssdlf+lostPwr, nowStr, id)
				updateStatements = append(updateStatements, sql2)
			} else {
				var insertList []string
				insertList = append(insertList, "UUID()")
				insertList = append(insertList, fmt.Sprintf("'%s'", devId))
				insertList = append(insertList, fmt.Sprintf("'%s'", HashKey))
				insertList = append(insertList, fmt.Sprintf("'%s'", begintimeiStr))
				insertList = append(insertList, fmt.Sprintf("'%s'", endTimeiStr))
				insertList = append(insertList, fmt.Sprintf("'%d'", code))
				insertList = append(insertList, fmt.Sprintf("'%s'", translostcode(code)))
				insertList = append(insertList, fmt.Sprintf("%f", lostPwr))                        //损失电量
				insertList = append(insertList, fmt.Sprintf("'%d'", listingcode))                  //挂牌编码
				insertList = append(insertList, fmt.Sprintf("'%s'", transguapaicode(listingcode))) //挂牌名称
				insertList = append(insertList, fmt.Sprintf("'%s'", nowStr))
				insertList = append(insertList, fmt.Sprintf("'%s'", nowStr))
				insertStmts = append(insertStmts, fmt.Sprintf("(%s)", strings.Join(insertList, ",")))

			}
		}
		if len(updateStatements) > 50000 {
			err := utils.ExecBatchMysql(updateStatements, nil)
			if err != nil {
				fmt.Println(err, "batch update")
			}
			updateStatements = nil
		}
		if len(insertStmts) > 50000 {
			sql3 := fmt.Sprintf("INSERT INTO scada_wind_power_lost VALUES %s", strings.Join(insertStmts, ","))
			err := utils.ExecMysql(sql3)
			if err != nil {
				fmt.Println(err, "insert")
			}
			insertStmts = nil
		}
	}
	if len(updateStatements) > 0 {
		err := utils.ExecBatchMysql(updateStatements, nil)
		if err != nil {
			fmt.Println(err, "batch update")
		}
	}
	if len(insertStmts) > 0 {
		sql3 := fmt.Sprintf("INSERT INTO scada_wind_power_lost VALUES %s", strings.Join(insertStmts, ","))
		err := utils.ExecMysql(sql3)
		if err != nil {
			fmt.Println(err, "insert")
		}
	}
}

// 全设备生命周期损失融合
func mergeAll(mergeMap map[string][][]any) map[string][][]any {
	resultMaps := make(map[string][][]any)
	//排序
	keys := make([]string, 0, len(mergeMap))
	for key := range mergeMap {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, HashKey := range keys {
		var resultMap [][]any
		for i := 0; i < len(mergeMap[HashKey]); i++ {
			begintime := mergeMap[HashKey][i][0].(int)
			endTime := mergeMap[HashKey][i][1].(int)
			code := mergeMap[HashKey][i][2].(int)
			overTag := mergeMap[HashKey][i][3].(int)
			lostPwr := mergeMap[HashKey][i][4].(float64)
			listingcode := mergeMap[HashKey][i][5].(int)
			if overTag == 1 || overTag == 2 || overTag == 3 {
				overTag += 0
			}
			if len(resultMap) == 0 {
				resultMap = append(resultMap, []any{begintime, endTime, code, overTag, lostPwr, listingcode})
				continue
			}
			lastResult := resultMap[len(resultMap)-1]
			lastBeginTime := lastResult[0].(int)
			lastCode := lastResult[2].(int)
			lastOverTag := lastResult[3].(int)
			lastLostPwr := lastResult[4].(float64)
			if lastCode == code && overTag == lastOverTag {
				maxListing := int(math.Max(float64(overTag), float64(lastOverTag)))
				resultMap[len(resultMap)-1] = []any{lastBeginTime, endTime, code, maxListing, lostPwr + lastLostPwr, listingcode}
			} else {
				if overTag == 1 || overTag == 2 || overTag == 3 {
					overTag += 0
				}
				resultMap = append(resultMap, []any{begintime, endTime, code, overTag, lostPwr, listingcode})
			}
		}
		resultMaps[HashKey] = resultMap
	}
	return resultMaps
}

// 状态&挂牌融合
func mergeTimeRange(stArrs, listtingSlice [][]int) [][]int {
	if listtingSlice == nil {
		return stArrs
	}
	for _, listting := range listtingSlice {
		code := listting[0]
		listingStart := listting[1]
		listingEnd := listting[2]
		overTag := listting[3]
		listingCode := listting[4]
		startExistTag := []int{0, 0}
		endExistTag := 0
		codeInnerLast := 0
		for i := len(stArrs) - 1; i >= 0; i-- {
			times := stArrs[i][0]
			codes := stArrs[i][1]
			if times <= listingEnd && times >= listingStart {
				codeInnerLast = codes
			}
			if times == listingStart {
				startExistTag[0] = 1
				startExistTag[1] = i
			}
			if times == listingEnd {
				endExistTag = 1
			}
		}
		if startExistTag[0] == 1 {
			stArrs[startExistTag[1]][0] = listingStart
			stArrs[startExistTag[1]][1] = code
			stArrs[startExistTag[1]][2] = overTag
		} else {
			stArrs = append(stArrs, []int{listingStart, code, overTag, listingCode})
		}
		if endExistTag != 1 {
			stArrs = append(stArrs, []int{listingEnd, codeInnerLast, 0, listingCode})
		}
		var filteredValues [][]int
		for _, i := range stArrs {
			if i[0] <= listingStart || i[0] >= listingEnd {
				filteredValues = append(filteredValues, i)
			}
		}
		stArrs = filteredValues
		sort.Slice(stArrs, func(i, j int) bool {
			return stArrs[i][0] < stArrs[j][0]
		})
	}
	return stArrs
}

// 挂牌记录切分
func findOverLap(listing [][]int, fromTime int, toTime int) [][]int {
	if len(listing) == 0 {
		return nil
	}
	var listingSlice [][]int
	for i := 0; i < len(listing); i++ {
		code := listing[i][0]
		recode := transFmt(code, "guapai")
		beginTimeSec := listing[i][1]
		endTimeSec := listing[i][2]
		overTag := listing[i][3]
		listingCode := listing[i][4]
		//寻找时间区间交集
		overlapStart := int(math.Max(float64(beginTimeSec), float64(fromTime)))
		overlapEnd := int(math.Min(float64(endTimeSec), float64(toTime)))
		if overlapStart < overlapEnd {
			listingSlice = append(listingSlice, []int{recode, overlapStart, overlapEnd, overTag, listingCode})
		}
	}
	return listingSlice
}

// GetListingData 获取挂牌信息
func GetListingData(dayTimeStr, todayTimeStr string) (map[string][][]int, error) {
	resultsDict := make(map[string][][]int)
	selectsql := "SELECT t.device,t.listingNo,t.realBgnTm,t.realEndTm from scada_listing_result_his t where realBgnTm >= ? and realEndTm <= ?"
	rows, err := utils.QueryMysql(selectsql, dayTimeStr, todayTimeStr)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var HashKey string
		var code int
		var beginTimeStr string
		var endTimeStr string
		err := rows.Scan(&HashKey, &code, &beginTimeStr, &endTimeStr)
		if err != nil {
			return nil, err
		}
		beginTime := int(utils.StrToTime(beginTimeStr).UnixMilli())
		endTime := int(utils.StrToTime(endTimeStr).UnixMilli())
		resultsDict[HashKey] = append(resultsDict[HashKey], []int{code, beginTime, endTime, 1, code})
	}
	sql2 := "SELECT t.device,t.listingNo,t.realBgnTm from scada_listing_result_his t where realEndTm is null and t.realBgnTm < ?"
	rows2, err := utils.QueryMysql(sql2, todayTimeStr)
	if err != nil {
		return nil, err
	}
	for rows2.Next() {
		var HashKey string
		var code int
		var beginTimeStr string
		err := rows2.Scan(&HashKey, &code, &beginTimeStr)
		if err != nil {
			return nil, err
		}
		beginTime := int(utils.StrToTime(beginTimeStr).UnixMilli())
		dayTime := int(utils.StrToTime(dayTimeStr).UnixMilli())
		endTime := int(utils.StrToTime(todayTimeStr).UnixMilli())
		if beginTime < dayTime {
			resultsDict[HashKey] = append(resultsDict[HashKey], []int{code, dayTime, endTime, 3, code})
		} else {
			resultsDict[HashKey] = append(resultsDict[HashKey], []int{code, beginTime, endTime, 2, code})
		}

	}
	return resultsDict, nil
}

// 状态码转换
func transFmt(code int, fmt string) int {
	if fmt == "st" {
		switch code {
		case 0: // 手动停机
			return 3
		case 1: // 正常发电
			return 1
		case 2: // 环境待命
			return 2
		case 3: // 维护状态
			return 3
		case 4: // 故障停机
			return 4
		case 5: // 未知状态
			return 5
		case 6: // 降出力运行
			return 6
		case 7: // 技术待命
			return 7
		case 8: // 电网故障
			return 8
		}
	} else if fmt == "guapai" {
		switch code {
		case 1: // 覆冰停机
			return 2
		case 2: // 调度限电
			return 9
		case 3: // 输变电计划停运
			return 11
		case 4: // 输变电非计划停运
			return 10
		case 5: // 暴风停机
			return 2
		case 6: // 环境超温
			return 2
		case 7: // 故障维护
			return 4
		case 8: // 定检维护
			return 3
		case 9: // 计划检修
			return 3
		case 10: // 机组故障
			return 4
		case 11: // 自降容
			return 6
		case 12: // 电网检修
			return 12
		case 13: // 电网故障
			return 8
		}
	}
	return -1
}

func transguapaicode(code int) string {
	switch code {
	case 1:
		return "覆冰停机"
	case 2:
		return "调度限电"
	case 3:
		return "输变电计划停运"
	case 4:
		return "输变电非计划停运"
	case 5:
		return "暴风停机"
	case 6:
		return "环境超温"
	case 7:
		return "故障维护"
	case 8:
		return "定检维护"
	case 9:
		return "计划检修"
	case 10:
		return "机组故障"
	case 11:
		return "自降容"
	case 12:
		return "电网检修"
	case 13:
		return "电网故障"
	}
	return ""
}

func translostcode(code int) string {
	switch code {
	case 1:
		return "正常发电损失"
	case 2:
		return "环境因素受累损失"
	case 3:
		return "风电机组计划停运损失"
	case 4:
		return "风电机组故障损失"
	case 5:
		return "未知损失"
	case 6:
		return "风电机组自降容损失"
	case 7:
		return "风电机组技术待命损失"
	case 8:
		return "电网故障损失"
	case 9:
		return "电网限电损失"
	case 10:
		return "输变电非计划停运损失"
	case 11:
		return "输变电计划停运损失"
	case 12:
		return "电网检修损失"
	}
	return ""
}
