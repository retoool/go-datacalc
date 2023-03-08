package main

import (
	"fmt"
	"github.com/robfig/cron"
	flag "github.com/spf13/pflag"
	"go-datacalc/datacalc"
	"go-datacalc/utils"
	"time"
)

func main() {
	var task string
	flag.StringVarP(&task, "task", "t", "", "The task")
	flag.Parse()
	fmt.Println("main()")
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
	err := c.AddFunc("10 0 * * *", func() {
		datacalc.Run()
		datacalc.RunPlusPoint()

		nowTime := time.Now()
		layout := "2006-01-02 15:04:05"
		t, err := time.Parse(layout, utils.TimeToStr(nowTime))
		if err != nil {
			fmt.Println("解析时间字符串失败：", err)
			return
		}
		if t.Day() == 1 {
			datacalc.DevCalcHisMonth(nowTime)
		}
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	c.Start()

	select {}
}

func RunHisCalc() {
	if utils.HisCalcBeginTime == "" || utils.HisCalcEndTime == "" {
		fmt.Println("未读取到配置文件")
		return
	}
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
		layout := "2006-01-02 15:04:05"
		t, err := time.Parse(layout, toTimeStr)
		if err != nil {
			fmt.Println("解析时间字符串失败：", err)
			return
		}
		if t.Day() == 1 {
			datacalc.DevCalcHisMonth(utils.StrToTime(toTimeStr))
		}
	}
}

func RunMonthHisCurve() {
	if utils.HisCurveCalcTime == "" {
		fmt.Println("未读取到配置文件")
		return
	}
	calcTimeStr := utils.HisCurveCalcTime
	fmt.Println("HisCurveCalcTime: " + calcTimeStr)
	calcTime := utils.StrToTime(calcTimeStr)
	datacalc.DevCalcHisMonth(calcTime)
}

func RunDeleteData() {
	if utils.DelDataBeginTime == "" || utils.DelDataEndTime == "" {
		fmt.Println("未读取到配置文件")
		return
	}
	beginTimeStr := utils.DelDataBeginTime
	endTimeStr := utils.DelDataEndTime
	datacalc.DeleteKdbData(beginTimeStr, endTimeStr)
}
