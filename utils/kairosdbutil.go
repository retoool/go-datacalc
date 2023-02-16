package utils

import (
	"fmt"
	"time"

	"github.com/retoool/go-kairosdb/builder"
	"github.com/retoool/go-kairosdb/builder/grouper"
	"github.com/retoool/go-kairosdb/builder/utils"
	"github.com/retoool/go-kairosdb/client"
)

func GetKairosdb10min(startTime, endTime time.Time, metrics []string, devCodes []string) {
	agg := builder.CreateAverageAggregator(10, utils.MINUTES)
	qb := builder.NewQueryBuilder()
	qb.SetAbsoluteStart(startTime).SetAbsoluteEnd(endTime)
	for _, metric := range metrics {
		qb.AddMetric(metric).AddAggregator(agg).AddTag("project", devCodes)
	}
	cli := client.NewHttpClient(KairosDb)
	queryResponse, err := cli.Query(qb)
	if err != nil {
		return
	}
	fmt.Println(queryResponse)
}

func GetKairosdb10minGroup(startTime, endTime time.Time, metrics []string) {
	agg := builder.CreateAverageAggregator(10, utils.MINUTES)
	qb := builder.NewQueryBuilder()
	qb.SetAbsoluteStart(startTime).SetAbsoluteEnd(endTime)
	for _, metric := range metrics {
		qb.AddMetric(metric).AddAggregator(agg).AddGrouper(grouper.NewTagsGroup([]string{"project"}))
	}
	cli := client.NewHttpClient(KairosDb)
	queryResponse, err := cli.Query(qb)
	if err != nil {
		return
	}
	fmt.Println(queryResponse)
}
