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

func GetKairosdb(aggr string, startTime, endTime time.Time, metrics []string, devCodes []string, samplingValue int, samplingUnit string, filter [][]string) *response.QueryResponse {
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
	if aggr == "avg" {
		agg = builder.CreateAverageAggregator(samplingValue, Unit)
	}
	if aggr == "max" {
		agg = builder.CreateMaxAggregator(samplingValue, Unit)
	}
	if aggr == "min" {
		agg = builder.CreateMinAggregator(samplingValue, Unit)
	}
	if aggr == "count" {
		agg = builder.CreateCountAggregator(samplingValue, Unit)
	}
	if aggr == "last" {
		agg = builder.CreateLastAggregator(samplingValue, Unit)
	}
	if aggr == "first" {
		agg = builder.CreateFirstAggregator(samplingValue, Unit)
	}
	if aggr == "gap" {
		agg = builder.CreateDataGapsMarkingAggregator(samplingValue, Unit)
	}
	if aggr == "leastSquares" {
		agg = builder.CreateLeastSquaresAggregator(samplingValue, Unit)
	}
	if aggr == "dev" {
		agg = builder.CreateStandardDeviationAggregator(samplingValue, Unit)
	}
	for i := 0; i < len(filter); i++ {
		s := filter[i][0]
		v := filter[i][1]
		float, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return nil
		}
		if s == "equal" {
			agg = builder.CreateFilterAggregator(builder.FilterOp_EQ, float)
		}
		if s == "lt" {
			agg = builder.CreateFilterAggregator(builder.FilterOp_LT, float)
		}
		if s == "lte" {
			agg = builder.CreateFilterAggregator(builder.FilterOp_LTE, float)
		}
		if s == "gt" {
			agg = builder.CreateFilterAggregator(builder.FilterOp_GT, float)
		}
		if s == "gte" {
			agg = builder.CreateFilterAggregator(builder.FilterOp_GTE, float)
		}
	}
	qb := builder.NewQueryBuilder()
	qb.SetAbsoluteStart(startTime).SetAbsoluteEnd(endTime)
	for _, metric := range metrics {
		qb.AddMetric(metric).AddAggregator(agg).AddTag("project", devCodes)
	}
	cli := client.NewHttpClient(KairosDb)
	resp, err := cli.Query(qb)
	if err != nil {
		return nil
	}
	return resp
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

func HandleRequest(response *response.QueryResponse) error{

	jsonData, err := json.Marshal(response)
	if err != nil {
		return nil
	}
	fmt.Println(jsonData)
	return nil
}
