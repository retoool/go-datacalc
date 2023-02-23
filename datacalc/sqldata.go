package datacalc

type Sqldata struct {
	devMap      map[string]V_scada_machine_group
	typeMap     map[string]Scada_wind_type
	fullcodeMap []string
}

func Newsqldata() *Sqldata{
	var s Sqldata
	s.devMap = Getdev()
	s.typeMap = Gettype()
	s.GetfullcodeMap()
	return &s
}
func (sqldata *Sqldata) GetfullcodeMap() {
	for key, _ := range sqldata.devMap {
		sqldata.fullcodeMap = append(sqldata.fullcodeMap, key)
	}
}
