package utils

import (
	"encoding/json"
	"fmt"
	"github.com/retoool/go-kairosdb/builder"
	"github.com/retoool/go-kairosdb/builder/grouper"
	"github.com/retoool/go-kairosdb/builder/utils"
	"github.com/retoool/go-kairosdb/client"
	"github.com/retoool/go-kairosdb/response"
	"strconv"
	"time"
)

func GetKdb(metric string, tags []string, aggr string, startTime, endTime time.Time, minvalue string, maxvalue string,
	samplingValue string, samplingUnit string) *map[string]map[int64]float64 {

	var Unit utils.TimeUnit
	if samplingUnit == "milliseconds" {
		Unit = utils.MILLISECONDS
	}
	if samplingUnit == "seconds" {
		Unit = utils.SECONDS
	}
	if samplingUnit == "minutes" {
		Unit = utils.MINUTES
	}
	if samplingUnit == "hours" {
		Unit = utils.HOURS
	}
	if samplingUnit == "days" {
		Unit = utils.DAYS
	}
	if samplingUnit == "weeks" {
		Unit = utils.WEEKS
	}
	if samplingUnit == "months" {
		Unit = utils.MONTHS
	}
	if samplingUnit == "years" {
		Unit = utils.YEARS
	}
	var agg builder.Aggregator
	samplingValuei, err := strconv.Atoi(samplingValue)
	if err != nil {
		fmt.Println(err)
	}
	if aggr == "avg" {
		agg = builder.CreateAverageAggregator(samplingValuei, Unit)
	}
	if aggr == "max" {
		agg = builder.CreateMaxAggregator(samplingValuei, Unit)
	}
	if aggr == "min" {
		agg = builder.CreateMinAggregator(samplingValuei, Unit)
	}
	if aggr == "count" {
		agg = builder.CreateCountAggregator(samplingValuei, Unit)
	}
	if aggr == "last" {
		agg = builder.CreateLastAggregator(samplingValuei, Unit)
	}
	if aggr == "first" {
		agg = builder.CreateFirstAggregator(samplingValuei, Unit)
	}
	if aggr == "gap" {
		agg = builder.CreateDataGapsMarkingAggregator(samplingValuei, Unit)
	}
	if aggr == "leastSquares" {
		agg = builder.CreateLeastSquaresAggregator(samplingValuei, Unit)
	}
	if aggr == "dev" {
		agg = builder.CreateStandardDeviationAggregator(samplingValuei, Unit)
	}
	//for i := 0; i < len(filter); i++ {
	//	s := filter[i][0]
	//	v := filter[i][1]
	//	float, err := strconv.ParseFloat(v, 64)
	//	if err != nil {
	//		return nil
	//	}
	//	if s == "equal" {
	//		agg = builder.CreateFilterAggregator(builder.FilterOp_EQ, float)
	//	}
	//	if s == "lt" {
	//		agg = builder.CreateFilterAggregator(builder.FilterOp_LT, float)
	//	}
	//	if s == "lte" {
	//		agg = builder.CreateFilterAggregator(builder.FilterOp_LTE, float)
	//	}
	//	if s == "gt" {
	//		agg = builder.CreateFilterAggregator(builder.FilterOp_GT, float)
	//	}
	//	if s == "gte" {
	//		agg = builder.CreateFilterAggregator(builder.FilterOp_GTE, float)
	//	}
	//}
	if minvalue != "" {
		float, err := strconv.ParseFloat(minvalue, 64)
		if err != nil {
			fmt.Println(err)
		}
		agg = builder.CreateFilterAggregator(builder.FilterOp_LT, float)
	}
	if maxvalue != "" {
		float, err := strconv.ParseFloat(maxvalue, 64)
		if err != nil {
			fmt.Println(err)
		}
		agg = builder.CreateFilterAggregator(builder.FilterOp_GT, float)
	}
	qb := builder.NewQueryBuilder()
	qb.SetAbsoluteStart(startTime).SetAbsoluteEnd(endTime)
	devCodes := make(map[string][]string)
	devCodes["project"] = tags
	group := builder.CreateTagsGroupBy([]string{"project"})
	qb.AddMetric(metric).AddAggregator(agg).AddTags(devCodes).AddGrouper(group)
	cli := client.NewHttpClient(KairosDb)
	resp, err := cli.Query(qb)
	//打印json
	marshal, err := json.Marshal(qb)
	fmt.Println(string(marshal))

	if err != nil {
		return nil
	}
	respMap := RespToMap(resp)
	return respMap
}

func GetKairosdb10minAVGGroup(startTime, endTime time.Time, metrics []string) *response.QueryResponse {
	agg := builder.CreateAverageAggregator(10, utils.MINUTES)
	qb := builder.NewQueryBuilder()
	qb.SetAbsoluteStart(startTime).SetAbsoluteEnd(endTime)
	for _, metric := range metrics {
		qb.AddMetric(metric).AddAggregator(agg).AddGrouper(grouper.NewTagsGroup([]string{"project"}))
	}
	cli := client.NewHttpClient(KairosDb)
	queryResponse, err := cli.Query(qb)
	if err != nil {
		return nil
	}
	return queryResponse
}

func HandleRequest(response *response.QueryResponse) error {
	jsonData, err := json.Marshal(response)
	if err != nil {
		return nil
	}
	fmt.Println(jsonData)
	return nil
}

func RespToMap(resp *response.QueryResponse) *map[string]map[int64]float64 {
	qrmap := make(map[string]map[int64]float64)
	for i := 0; i < len(resp.QueriesArr[0].ResultsArr); i++ {
		results := resp.QueriesArr[0].ResultsArr[i]
		points := results.DataPoints
		tag := results.Tags["project"][0]
		if qrmap[tag] == nil {
			qrmap[tag] = make(map[int64]float64)
		}
		for y := 0; y < len(points); y++ {
			value, err := points[y].Float64Value()
			if err != nil {
				fmt.Println(err)
			}
			timestamp := points[y].Timestamp()
			qrmap[tag][timestamp] = value
		}
	}
	return &qrmap
}
