package datacalc

import (
	"encoding/json"
	"fmt"
	"go-datacalc/utils"
	"go-datacalc/utils/kdb"
	"sort"
	"strings"
	"time"
)

func GetPowerCurveHis() (map[string][][]float64, error) {
	now := time.Now()
	thisMonth := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.Local)
	thisMonthStr := thisMonth.Format("2006-01-02")
	querysql := "select wind_code, power_curve_his from `scada_wind_power_curve_his` where curve_date = ?"
	rows, err := utils.QueryMysql(querysql, thisMonthStr)
	if err != nil {
		return nil, err
	}
	powerCurveDict := make(map[string][][]float64)
	for rows.Next() {
		var windCode string
		var powerCurveHis string
		err := rows.Scan(&windCode, &powerCurveHis)
		if err != nil {
			return nil, err
		}
		var powerCurve [][]float64
		dataDict := make(map[string]string)
		err = json.Unmarshal([]byte(powerCurveHis), &dataDict)
		if err != nil {
			return nil, err
		}
		for spdStr, pwrStr := range dataDict {
			spd, err := utils.StrToFloat(spdStr)
			if err != nil {
				continue
			}
			pwr, err := utils.StrToFloat(pwrStr)
			if err != nil {
				continue
			}
			powerCurve = append(powerCurve, []float64{spd, pwr})
		}
		powerCurveDict[windCode] = powerCurve
	}
	return powerCurveDict, nil
}

func DevCalcHisMonth() {
	now := time.Now()
	thisMonth := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location())
	beginTime := thisMonth.AddDate(0, -3, 0)
	endTime := thisMonth
	s := GetSqlDataInstance()
	codeMap := s.codeSlice
	windSpdMap := kdb.QueryKdb("WNAC_WdSpd_Interval_10m", codeMap, "sum", beginTime, endTime, "", "", "", "1", "milliseconds")
	pwrMap := kdb.QueryKdb("ActPWR_Fitting_AVG_10m", codeMap, "sum", beginTime, endTime, "", "", "", "1", "milliseconds")
	spdDict := make(map[string]map[int]float64)
	pwrDict := make(map[string]map[int]float64)
	sumDict := make(map[string]map[float64][]float64)
	for key := range windSpdMap {
		if spdDict[key] == nil {
			spdDict[key] = make(map[int]float64)
		}
		for i := 0; i < len(windSpdMap[key]); i++ {
			timestamp, err := utils.StrToInt(windSpdMap[key][i][0])
			if err != nil {
				fmt.Println(err)
				continue
			}
			value, err := utils.StrToFloat(windSpdMap[key][i][1])
			if err != nil {
				fmt.Println(err)
				continue
			}
			windType := s.devMap[key].machineTypeCode
			windSpeedCutIn, err := utils.StrToFloat(s.typeMap[windType].windSpeedCutIn)
			if err != nil {
				fmt.Println(err)
			}
			windSpeedCutOut, err := utils.StrToFloat(s.typeMap[windType].windSpeedCutOut)
			if err != nil {
				fmt.Println(err)
			}
			if value >= windSpeedCutIn && value <= windSpeedCutOut {
				spdDict[key][timestamp] = value
			}
		}
	}
	for key := range pwrMap {
		if pwrDict[key] == nil {
			pwrDict[key] = make(map[int]float64)
		}
		for i := 0; i < len(pwrMap[key]); i++ {
			timestamp, err := utils.StrToInt(pwrMap[key][i][0])
			if err != nil {
				fmt.Println(err)
				continue
			}
			value, err := utils.StrToFloat(pwrMap[key][i][1])
			if err != nil {
				fmt.Println(err)
				continue
			}
			pwrDict[key][timestamp] = value
			if spdDict[key] != nil {
				if spdDict[key][timestamp] != 0 {
					windSpd := spdDict[key][timestamp]
					if sumDict[key] == nil {
						sumDict[key] = make(map[float64][]float64)
					}
					if sumDict[key][windSpd] == nil {
						sumDict[key][windSpd] = []float64{0, 0}
					}
					sumDict[key][windSpd][0] += value
					sumDict[key][windSpd][1] += 1
				}
			}
		}
	}
	// 将键放入切片中
	keys := make([]string, 0, len(sumDict))
	for key := range sumDict {
		keys = append(keys, key)
	}

	// 对切片进行排序
	sort.Strings(keys)

	// 根据排好序的键遍历map
	for _, key := range keys {
		resultDict := make(map[string]string)
		for windspd := range sumDict[key] {
			value := sumDict[key][windspd][0] / sumDict[key][windspd][1]
			resultDict[utils.FloatToStr(windspd, 1)] = utils.FloatToStr(value, 6)
		}
		windCode := key
		windType := s.devMap[key].machineTypeCode
		curveDate := thisMonth.Format("2006-01-02")
		powerCurveHisjson, err := json.Marshal(resultDict)
		if err != nil {
			fmt.Println(err)
			break
		}
		powerCurveHis := string(powerCurveHisjson)

		selectSql := "SELECT id FROM scada_wind_power_curve_his WHERE wind_code = ? AND curve_date = ?"
		rows, err := utils.QueryMysql(selectSql, windCode, curveDate)
		if err != nil {
			fmt.Println(err)
			break
		}
		var recid string
		for rows.Next() {
			err := rows.Scan(&recid)
			if err != nil {
				fmt.Println(err)
			}
		}
		if recid != "" {
			updateSql := "UPDATE scada_wind_power_curve_his SET power_curve_his = ? WHERE id = ?"
			err = utils.ExecMysql(updateSql, powerCurveHis, recid)
			if err != nil {
				fmt.Println(err)
				break
			}
		} else {
			insertList := []string{}
			insertList = append(insertList, "UUID()")
			insertList = append(insertList, fmt.Sprintf("'%s'", windCode))
			insertList = append(insertList, fmt.Sprintf("'%s'", windType))
			insertList = append(insertList, fmt.Sprintf("'%s'", curveDate))
			insertList = append(insertList, fmt.Sprintf("'%s'", powerCurveHis))
			insertSQL := fmt.Sprintf("insert into `scada_wind_power_curve_his` value (%s)", strings.Join(insertList, ","))
			err := utils.ExecMysql(insertSQL)
			if err != nil {
				fmt.Println(err)
			}
		}

	}

}
