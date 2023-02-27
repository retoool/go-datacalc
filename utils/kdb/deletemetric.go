package kdb

import (
	"fmt"
	"go-datacalc/utils/kdb/entity"
	"net/http"
	"time"
)

func DeteleMetric(pointname string, tags []string, aggr string, starttime time.Time, endtime time.Time,
	aligntime string, minvalue string, maxvalue string, samplingValue string, samplingUnit string) *http.Response {
	beginunix := starttime.Unix()
	endUnix := endtime.Unix()
	k := entity.NewKairosdb()
	if samplingValue == "" && samplingUnit == "" {
		samplingValue = "10"
		samplingUnit = "years"
	}
	bodytext := make(map[string]interface{})
	if tags == nil {
		bodytext = map[string]interface{}{
			"start_absolute": beginunix*1000 + 1,
			"end_absolute":   endUnix * 1000,
			"metrics": []map[string]interface{}{
				{
					"group_by": []map[string]interface{}{
						{"name": "tag", "tags": []string{"project"}},
					},
					"name":        pointname,
					"tags":        map[string]interface{}{},
					"aggregators": []interface{}{},
				},
			},
		}
	} else {
		bodytext = map[string]interface{}{
			"start_absolute": beginunix*1000 + 1,
			"end_absolute":   endUnix * 1000,
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
	response, err := entity.SendRequest(k.DeleteUrl, bodytext, k.Headersjson)
	if err != nil {
		fmt.Println(err)
	}
	return response
}
