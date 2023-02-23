package utils

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
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
	aligntime string, minvalue string, maxvalue string, samplingValue string, samplingUnit string) *map[string]map[int64]float64{
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
	result, err := sendRequest(k.Url, bodytext, k.headers)
	if err != nil {
		fmt.Println("Error:",err)
		return nil
	}
	var qrmap map[string]map[int64]float64
	for i := 0; i <len(result.QueriesArr[0].ResultsArr); i++ {
		results := result.QueriesArr[0].ResultsArr[i]
		points := results.DataPoints
		tag := results.Tags["project"][0]
		for y := 0; y < len(points); y++ {
			value,err := points[y].Float64Value()
			if err != nil {
				fmt.Println(err)
			}
			timestamp := points[y].Timestamp()
			qrmap[tag][timestamp] = value
		}
	}
	return &qrmap
}

func sendRequest(url string, bodytext interface{}, headers map[string]string) (*QueryResponse, error) {
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
	response, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	contents, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	qr := NewQueryResponse(response.StatusCode)
	err = json.Unmarshal(contents, qr)
	if err != nil {
		return nil, err
	}
	return qr, nil
}
type GroupResult struct {
	Name string `json:"name,omitempty"`
}

type Results struct {
	Name       string              `json:"name,omitempty"`
	DataPoints []DataPoint `json:"values,omitempty"`
	Tags       map[string][]string `json:"tags,omitempty"`
	Group      []GroupResult       `json:"group_by,omitempty"`
}

type Queries struct {
	SampleSize int64     `json:"sample_size,omitempty"`
	ResultsArr []Results `json:"results,omitempty"`
}

type QueryResponse struct {
	*Response
	QueriesArr []Queries `json:"queries,omitempty"`
}
func NewQueryResponse(code int) *QueryResponse {
	qr := &QueryResponse{
		Response: &Response{},
	}

	qr.SetStatusCode(code)
	return qr
}

type Response struct {
	statusCode int
	Errors     []string `json:"errors,omitempty"`
}

func (r *Response) SetStatusCode(code int) {
	r.statusCode = code
}

func (r *Response) GetStatusCode() int {
	return r.statusCode
}

func (r *Response) GetErrors() []string {
	return r.Errors
}

// Represents a measurement. Stores the time when the measurement occurred and its value.
type DataPoint struct {
	timestamp int64
	value     interface{}
}

func NewDataPoint(ts int64, val interface{}) *DataPoint {
	return &DataPoint{
		timestamp: ts,
		value:     val,
	}
}

func (dp *DataPoint) Timestamp() int64 {
	return dp.timestamp
}

func (dp *DataPoint) Int64Value() (int64, error) {
	val, ok := dp.value.(int64)
	if !ok {
		v, ok := dp.value.(int)
		if !ok {
			return 0, errors.New("ErrorDataPointInt64")
		}
		val = int64(v)
	}

	return val, nil
}

func (dp *DataPoint) Float64Value() (float64, error) {
	val, ok := dp.value.(float64)
	if !ok {
		return 0,errors.New("ErrorDataPointFloat64")
	}
	return val, nil
}

//20191101 add by wutz (no need)
func (dp *DataPoint) Float32Value() (float32, error) {
	val, ok := dp.value.(float32)
	if !ok {
		return 0, errors.New("ErrorDataPointFloat32")
	}
	return val, nil
}

func (dp *DataPoint) MarshalJSON() ([]byte, error) {
	data := []interface{}{dp.timestamp, dp.value}
	return json.Marshal(data)
}

func (dp *DataPoint) UnmarshalJSON(data []byte) error {
	var arr []interface{}
	err := json.Unmarshal(data, &arr)
	if err != nil {
		return err
	}

	var v float64
	ok := false
	if v, ok = arr[0].(float64); !ok {
		return errors.New("Invalid Timestamp type")
	}

	// Update the receiver with the values decoded.
	dp.timestamp = int64(v)
	dp.value = arr[1]

	return nil
}

