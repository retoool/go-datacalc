package datacalc

import "sync"

type SqlData struct {
	devMap    map[string]V_scada_machine_group
	typeMap   map[string]Scada_wind_type
	codeSlice []string
}

var instanceSqlData *SqlData
var onceSqlData sync.Once

func GetSqlDataInstance() *SqlData {
	onceSqlData.Do(func() {
		instanceSqlData = &SqlData{
			devMap:    Getdev(),
			typeMap:   Gettype(),
			codeSlice: GetFullCodeMap(),
		}
	})
	return instanceSqlData
}
func GetFullCodeMap() []string {
	var codeSlice []string
	for key, _ := range Getdev() {
		codeSlice = append(codeSlice, key)
	}
	return codeSlice
}
