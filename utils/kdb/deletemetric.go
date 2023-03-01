package kdb

import (
	"fmt"
	"go-datacalc/utils/kdb/entity"
	"net/http"
	"time"
)

func DeteleMetric(pointname string, starttime time.Time, endtime time.Time) *http.Response {
	beginunix := starttime.Unix()
	endUnix := endtime.Unix()
	k := entity.NewKairosdb()
	bodytext := make(map[string]interface{})

	bodytext = map[string]interface{}{
		"start_absolute": beginunix*1000 + 1,
		"end_absolute":   endUnix * 1000,
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
