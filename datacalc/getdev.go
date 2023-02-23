package datacalc

import (
	"database/sql"
	"fmt"
	"go-datacalc/utils"
	"strings"
)

type V_scada_machine_group struct {
	code            string
	machineTypeCode string
	lineCode        string
	salveName       string
	project         string
	farm            string
	term            string
	dev             string
	altitude        sql.NullString
	hubHeight       sql.NullString
}
type security_organization struct {
	id        string
	code      string
	parent_id string
}
type scada_wind_machine struct {
	id          string
	orgId       string
	machineCode string
	altitude    sql.NullString
	hubHeight   sql.NullString
}

//	func Outxml() {
//		//filename := "config/xy_devStCalc.xml"
//		//_, err := os.Stat(filename)
//		//if os.IsNotExist(err) {
//		//	fmt.Printf("%s does not exist, creating...\n", filename)
//		//	file, err := os.Create(filename)
//		//	if err != nil {
//		//		fmt.Printf("Error creating %s: %s\n", filename, err)
//		//		return
//		//	}
//		//	err = file.Close()
//		//	if err != nil {
//		//		panic(err)
//		//		return
//		//	}
//		//	fmt.Printf("%s created successfully.\n", filename)
//		//} else {
//		//	fmt.Printf("%s opened successfully.\n", filename)
//		//}
//		getdevattr()
//	}
func Getdev() map[string]V_scada_machine_group{
	fullcodedict,err := getwindhigh()
	if err != nil {
		fmt.Println(err)
	}
	strSQL := "SELECT CODE, MachineTypeCode, line_code, SalveName from v_scada_machine_group WHERE MachineTypeName ='风电' ORDER BY CODE "
	rows, err := utils.QueryMysql(strSQL)
	if err != nil {
		fmt.Println(err)
	}
	devmap := make(map[string]V_scada_machine_group)
	for rows.Next() {
		var v V_scada_machine_group
		err := rows.Scan(&v.code, &v.machineTypeCode, &v.lineCode, &v.salveName)
		if err != nil {
			fmt.Println(err)
		}
		CODEList := strings.Split(v.code, ":")
		v.project = CODEList[0]
		v.farm = CODEList[1]
		v.term = CODEList[2]
		v.dev = CODEList[3]
		v.altitude = fullcodedict[v.code]["altitude"]
		v.hubHeight = fullcodedict[v.code]["hubHeight"]
		devmap[v.code] = v
	}

	return devmap
}

func getwindhigh() (map[string]map[string]sql.NullString,error) {
	SQL1 := "select t1.id, t1.code, t1.PARENT_ID from security_organization as t1 where t1.nature is not null and t1.enabled = 1"
	rows1, err := utils.QueryMysql(SQL1)
	if err != nil {
		fmt.Println(err)
	}
	orgcodedict := make(map[string]string)
	for rows1.Next() {
		var o security_organization
		err := rows1.Scan(&o.id, &o.code, &o.parent_id)
		if err != nil {
			fmt.Println(err)
		}
		orgcode,err := getorgcode(o.parent_id, o.code, rows1)
		if err != nil {
			return nil,err
		}
		orgcodedict[o.id] = orgcode
	}
	SQL2 := "SELECT id, org_id, machine_code, altitude, hubHeight FROM `scada_wind_machine`"
	rows2, err := utils.QueryMysql(SQL2)
	if err != nil {
		fmt.Println(err)
	}
	fullcodedict := make(map[string]map[string]sql.NullString)
	for rows2.Next() {
		var m scada_wind_machine
		err := rows2.Scan(&m.id, &m.orgId, &m.machineCode, &m.altitude, &m.hubHeight)
		if err != nil {
			fmt.Println(err)
		}
		fullcode := orgcodedict[m.orgId] + ":" + m.machineCode
		if fullcodedict[fullcode] == nil {
			fullcodedict[fullcode] = make(map[string]sql.NullString)
		}
		fullcodedict[fullcode]["altitude"] = m.altitude
		fullcodedict[fullcode]["hubHeight"] = m.hubHeight
	}
	return fullcodedict,nil
}
func getorgcode(parentid string, code string, sqlResult *sql.Rows) (string,error) {
	for sqlResult.Next() {
		var o security_organization
		err := sqlResult.Scan(&o.id, &o.code, &o.parent_id)
		if err != nil {
			fmt.Println(err)
		}
		id := o.id
		newparentid := o.parent_id
		if id == parentid && o.code != "root" {
			newcode := o.code + ":" + code
			code,err = getorgcode(newparentid, newcode, sqlResult)
			if err != nil {
				fmt.Println(err)
			}
		}
	}
	return code,nil
}
