package datacalc

import "sync"

type SqlData struct {
	DevMap    map[string]V_scada_machine_group
	TypeMap   map[string]Scada_wind_type
	CodeSlice []string
}

var instanceSqlData *SqlData
var onceSqlData sync.Once

func GetSqlDataInstance() *SqlData {
	onceSqlData.Do(func() {
		instanceSqlData = &SqlData{
			DevMap:    Getdev(),
			TypeMap:   Gettype(),
			CodeSlice: GetFullCodeMap(),
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
