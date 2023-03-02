package kdb

import (
	"fmt"
	"go-datacalc/utils/kdb/entity"
	"net/http"
	"time"
)

func DeteleMetricRange(pointname string, starttime time.Time, endtime time.Time) *http.Response {
	beginunix := starttime.UnixMilli()
	endUnix := endtime.UnixMilli()
	k := entity.NewKairosdb()
	bodytext := make(map[string]interface{})

	bodytext = map[string]interface{}{
		"start_absolute": beginunix,
		"end_absolute":   endUnix,
		"metrics": []map[string]interface{}{
			{
				"name": pointname,
			},
		},
	}
	response, err := entity.SendRequest(k.DeleteUrl, bodytext, k.Headersjson)
	if err != nil {
		fmt.Println(err)
	}
	return response
}

func DeteleMetric(pointName string) *http.Response {
	k := entity.NewKairosdb()
	req, err := http.NewRequest("DELETE", k.DelUrl+pointName, nil)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	client := &http.Client{}
	response, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	return response
}
