package entity

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go-datacalc/utils"
	"net/http"
)

type Kairosdb struct {
	Server   string
	Port     string
	QueryUrl string
	PushUrl  string
	DelUrl   string
	Headers  map[string]string
}

func NewKairosdb() Kairosdb {
	var k Kairosdb
	k.Server = utils.KairosdbHost
	k.Port = utils.KairosdbPort
	k.QueryUrl = fmt.Sprintf("http://%s:%s/api/v1/datapoints/query", k.Server, k.Port)
	k.PushUrl = fmt.Sprintf("http://%s:%s/api/v1/datapoints", k.Server, k.Port)
	k.DelUrl = fmt.Sprintf("http://%s:%s/api/v1/metric/", k.Server, k.Port)
	k.Headers = map[string]string{"content-type": "application/json"}
	return k
}
func SendRequest(url string, bodyText interface{}, headers map[string]string) (*http.Response, error) {
	jsonBody, err := json.Marshal(bodyText)
	fmt.Println(string(jsonBody))
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
	return response, nil
}
