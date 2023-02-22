package utils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"
)

type Kairosdb struct {
	strserver string
	strport   string
	Url       string
	DelUrl    string
	headers   map[string]string
}

func (kairosdb *Kairosdb) NewKairosdb() {
	kairosdb.strserver = KairosdbHost
	kairosdb.strport = KairosdbPort
	kairosdb.Url = fmt.Sprintf("http://%s:%s/api/v1/datapoints/query", kairosdb.strserver, kairosdb.strport)
	kairosdb.DelUrl = fmt.Sprintf("http://%s:%s/api/v1/metric/", kairosdb.strserver, kairosdb.strport)
	kairosdb.headers = map[string]string{"content-type": "application/json"}
}
func KairosdbClient(pointname string, tags []string, aggr string, starttime time.Time, endtime time.Time,
	aligntime string, minvalue string, maxvalue string, tagMatch, samplingValue string, samplingUnit string) {
	beginunix := starttime.Unix()
	endUnix := endtime.Unix()
	var k Kairosdb
	k.NewKairosdb()
	if samplingValue == "0" && samplingUnit == "" {
		samplingValue = "10"
		samplingUnit = "years"
	}
	bodytext := map[string]interface{}{
		"start_absolute": beginunix * 1000,
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
	//bodytextJson, err := json.Marshal(bodytext)
	//if err != nil {
	//	panic(err)
	//}
	//fmt.Println(string(bodytextJson))
	response, err := sendRequest(k.Url, bodytext, k.headers)
	if err != nil {
		panic(err)
	}
	defer response.Body.Close()
	var buf bytes.Buffer
	_, err = io.Copy(&buf, response.Body)
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		panic(err)
	}
	fmt.Println(buf.String())

	var data []map[string]interface{}
	err = json.Unmarshal(buf.Bytes(), &data)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Println(string(data))
}

func sendRequest(url string, bodytext interface{}, headers map[string]string) (*http.Response, error) {
	jsonBody, err := json.Marshal(bodytext)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, err
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}
