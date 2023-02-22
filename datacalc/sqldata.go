package datacalc

type Sqldata struct {
	devMap      map[string]V_scada_machine_group
	typeMap     map[string]Scada_wind_type
	fullcodeMap []string
}

func (sqldata *Sqldata) Newsqldata() {
	sqldata.devMap = Getdev()
	sqldata.typeMap = Gettype()
	sqldata.GetfullcodeMap()
}
func (sqldata *Sqldata) GetfullcodeMap() {
	for key, _ := range sqldata.devMap {
		sqldata.fullcodeMap = append(sqldata.fullcodeMap, key)
	}
}
