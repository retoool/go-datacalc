package main

import (
	"fmt"
	"github.com/robfig/cron"
	flag "github.com/spf13/pflag"
	"go-datacalc/datacalc"
	"go-datacalc/utils"
)

func main() {
	var task string
	flag.StringVarP(&task, "task", "t", "", "The task")
	flag.Parse()
	switch task {
	case "":
		RunCron()
	case "hiscalc":
		RunHisCalc()
	case "hiscurve":
		RunMonthHisCurve()
	case "deldata":
		RunDeleteData()
	}
}

func RunCron() {
	c := cron.New()
	// 每天0点10分触发
	err := c.AddFunc("10 0 * * *", func() {
		datacalc.Run()
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	// 每月1日0点10分触发
	err = c.AddFunc("10 0 1 * *", func() {
		datacalc.ThisMonthhisCurve()
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	c.Start()
}

func RunHisCalc() {
	beginTimeStr := utils.HisCalcBeginTime
	endTimeStr := utils.HisCalcEndTime
	beginTime := utils.StrToTime(beginTimeStr)
	endTime := utils.StrToTime(endTimeStr)
	frequency := 24 * 60 * 60
	timeRanges := utils.SplitTimeRanges(beginTime, endTime, frequency)
	fmt.Println(timeRanges)
	for _, ranges := range timeRanges {
		fromTimeStr := ranges[0]
		toTimeStr := ranges[1]
		datacalc.HisCalc(fromTimeStr, toTimeStr)
	}
}

func RunMonthHisCurve() {
	calcTimeStr := utils.HisCurveCalcTime
	fmt.Println("HisCurveCalcTime: " + calcTimeStr)
	calcTime := utils.StrToTime(calcTimeStr)
	datacalc.DevCalcHisMonth(calcTime)
}

func RunDeleteData() {
	beginTimeStr := utils.DelDataBeginTime
	endTimeStr := utils.DelDataEndTime
	datacalc.DeleteKdbData(beginTimeStr, endTimeStr)
}
