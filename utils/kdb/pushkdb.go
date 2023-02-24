package kdb

import (
	"fmt"
	"go-datacalc/utils/kdb/entity"
	"strconv"
)

type PushData struct {
	Name       string            `json:"name"`
	DataPoints [][]any           `json:"datapoints"`
	Tags       map[string]string `json:"tags"`
}

func NewData(devName string, dataPoints [][]any, tags map[string]string) PushData {
	p := PushData{
		Name:       devName,
		DataPoints: dataPoints,
		Tags:       tags,
	}
	return p
}
func PushKdb(pointName string, datas map[string][][]string) {
	var bodys []PushData
	for devName := range datas {
		var dataPoints [][]any
		for i := 0; i < len(datas[devName]); i++ {
			timestampstr := datas[devName][i][0]
			timestamp, err := strconv.Atoi(timestampstr)
			if err != nil {
				fmt.Println(err)
			}
			valuestr := datas[devName][i][1]
			value, err := strconv.ParseFloat(valuestr, 64)
			if err != nil {
				fmt.Println(err)
			}
			dataPoint := []any{timestamp, value}
			dataPoints = append(dataPoints, dataPoint)
		}
		tags := make(map[string]string)
		tags["project"] = devName
		body := NewData(pointName, dataPoints, tags)
		bodys = append(bodys, body)
	}
	k := entity.NewKairosdb()
	response, err := entity.SendRequest(k.PushUrl, bodys, k.Headersjson)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(response.StatusCode)
}
