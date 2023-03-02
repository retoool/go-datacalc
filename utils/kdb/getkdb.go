package kdb

import (
	"encoding/json"
	"fmt"
	"go-datacalc/utils/kdb/entity"
	"io/ioutil"
	"strconv"
	"time"
)

func QueryKdb(pointname string, tags []string, aggr string, starttime time.Time, endtime time.Time,
	aligntime string, minvalue string, maxvalue string, samplingValue string, samplingUnit string) map[string][][]string {
	beginunix := starttime.UnixMilli()
	endUnix := endtime.UnixMilli()
	k := entity.NewKairosdb()
	if samplingValue == "" && samplingUnit == "" {
		samplingValue = "10"
		samplingUnit = "years"
	}
	bodytext := map[string]interface{}{
		"start_absolute": beginunix,
		"end_absolute":   endUnix,
		"metrics": []map[string]interface{}{
			{
				"group_by": []map[string]interface{}{
					{"name": "tag", "tags": []string{"project"}},
				},
				"name": pointname,
				"tags": map[string]interface{}{
					"project": tags,
				},
				"aggregators": []interface{}{},
			},
		},
	}
	if minvalue != "" {
		minAggregator := map[string]interface{}{
			"name":      "filter",
			"filter_op": "lt",
			"threshold": minvalue,
		}
		bodytext["metrics"].([]map[string]interface{})[0]["aggregators"] = append(
			bodytext["metrics"].([]map[string]interface{})[0]["aggregators"].([]interface{}),
			minAggregator,
		)
	}
	if maxvalue != "" {
		maxAggregator := map[string]interface{}{
			"name":      "filter",
			"filter_op": "gt",
			"threshold": maxvalue,
		}
		bodytext["metrics"].([]map[string]interface{})[0]["aggregators"] = append(
			bodytext["metrics"].([]map[string]interface{})[0]["aggregators"].([]interface{}),
			maxAggregator,
		)
	}
	if aggr != "" {
		newAggregator := map[string]interface{}{
			"name":     aggr,
			"sampling": map[string]string{"value": samplingValue, "unit": samplingUnit},
		}
		bodytext["metrics"].([]map[string]interface{})[0]["aggregators"] = append(
			bodytext["metrics"].([]map[string]interface{})[0]["aggregators"].([]interface{}),
			newAggregator,
		)
	}
	if aligntime == "start" {
		aggregators := bodytext["metrics"].([]map[string]interface{})[0]["aggregators"].([]interface{})
		lastAggregator := aggregators[len(aggregators)-1]
		switch a := lastAggregator.(type) {
		case map[string]interface{}:
			a["align_start_time"] = true
		}
	} else if aligntime == "end" {
		aggregators := bodytext["metrics"].([]map[string]interface{})[0]["aggregators"].([]interface{})
		lastAggregator := aggregators[len(aggregators)-1]
		switch a := lastAggregator.(type) {
		case map[string]interface{}:
			a["align_end_time"] = true
		}
	}
	response, err := entity.SendRequest(k.QueryUrl, bodytext, k.Headersjson)
	contents, err := ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Println(err)
	}
	qr := entity.NewQueryResponse(response.StatusCode)
	err = json.Unmarshal(contents, qr)
	if err != nil {
		fmt.Println(err)
	}

	return RespToMap(qr)
}
func RespToMap(resp *entity.QueryResponse) map[string][][]string {
	qrMap := make(map[string][][]string)
	for i := 0; i < len(resp.QueriesArr[0].ResultsArr); i++ {
		results := resp.QueriesArr[0].ResultsArr[i]
		points := results.DataPoints
		if len(points) == 0 {
			fmt.Println(results.Name + ",该点没有数据")
			return nil
		}
		tag := results.Tags["project"][0]

		for y := 0; y < len(points); y++ {
			value, err := points[y].Float64Value()
			valuestr := fmt.Sprintf("%.6f", value)
			if err != nil {
				fmt.Println(err)
			}
			timestamp := points[y].Timestamp()
			timestampstr := strconv.Itoa(int(timestamp))
			qrMap[tag] = append(qrMap[tag], []string{timestampstr, valuestr})
		}
	}
	return qrMap
}
