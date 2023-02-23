package datacalc

import "go-datacalc/utils"

type Scada_wind_type struct {
	id              string
	windType        string
	name            string
	capacity        string
	windSpeedCutIn  string
	windSpeedCutOut string
	powerCurve      []scada_theory_power_curves
}
type scada_theory_power_curves struct {
	speed string
	power string
}

func Gettype() map[string]Scada_wind_type {
	strSQL1 := "SELECT a.id,a.wind_type,b.name,a.capacity,a.windspeed_cutin,a.windspeed_cutout FROM `scada_wind_type` as a JOIN (SELECT id,name FROM scada_wind_factory)as b on a.wind_factory=b.id ORDER BY b.name,a.capacity"
	rows1, err := utils.QueryMysql(strSQL1)
	if err != nil {
		return nil
	}
	typeMap := make(map[string]Scada_wind_type)
	for rows1.Next() {
		var t Scada_wind_type
		err := rows1.Scan(&t.id, &t.windType, &t.name, &t.capacity, &t.windSpeedCutIn, &t.windSpeedCutOut)
		if err != nil {
			return nil
		}
		strSQL2 := "SELECT speed,power FROM `scada_theory_power_curves` WHERE wind_type=? ORDER BY speed"
		rows2, err := utils.QueryMysql(strSQL2, t.id)
		if err != nil {
			return nil
		}
		for rows2.Next() {
			var p scada_theory_power_curves
			err := rows2.Scan(&p.speed, &p.power)
			if err != nil {
				return nil
			}
			t.powerCurve = append(t.powerCurve, p)
		}
		typeMap[t.windType] = t
	}
	return typeMap
}
