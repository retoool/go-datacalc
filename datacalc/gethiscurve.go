package datacalc

import (
	"encoding/json"
	"go-datacalc/utils"
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
