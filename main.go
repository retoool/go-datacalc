package main

import (
	"fmt"
	cron "github.com/robfig/cron/v3"
	flag "github.com/spf13/pflag"
	"go-datacalc/datacalc"
	"go-datacalc/utils"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/pprof"
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
func runpprof() {
	//http://localhost:6060/debug/pprof/
	//go tool pprof http://localhost:6060/debug/pprof/profile

	runtime.GOMAXPROCS(1)              // 限制 CPU 使用数，避免过载
	runtime.SetMutexProfileFraction(1) // 开启对锁调用的跟踪
	runtime.SetBlockProfileRate(1)     // 开启对阻塞操作的跟踪

	go func() {
		// 启动一个 http server，注意 pprof 相关的 handler 已经自动注册过了
		if err := http.ListenAndServe(":6060", nil); err != nil {
			log.Fatal(err)
		}
		os.Exit(0)
	}()
}

// go tool pprof heap.prof
func testpprof() {
	f, err := os.Create("cpu.prof")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer f.Close()

	if err := pprof.StartCPUProfile(f); err != nil {
		fmt.Println(err)
		return
	}
	defer pprof.StopCPUProfile()

	// 运行您的计算进程
	datacalc.Run()
	utils.GetCacheInstance().CacheData = nil
	utils.GetMsgInstance().Msg = nil

	datacalc.Run()
	utils.GetCacheInstance().CacheData = nil
	utils.GetMsgInstance().Msg = nil

	// 创建一个heap profile
	f, err = os.Create("heap.prof")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer f.Close()

	pprof.WriteHeapProfile(f)
}

func RunCron() {
	runpprof()
	c := cron.New()
	c.AddFunc("10 0 * * *", func() {
		datacalc.Run()
		nowTime := time.Now()
		if nowTime.Day() == 1 {
			datacalc.DevCalcHisMonth(nowTime)
		}
	})
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
		toTime := utils.StrToTime(toTimeStr)
		if toTime.Day() == 1 {
			datacalc.DevCalcHisMonth(toTime)
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
