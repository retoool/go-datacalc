package main

import (
	"fmt"
	"go-datacalc/utils"
	"time"
)

func main() {
	fmt.Println("main() run")
	t := utils.GetNowTime()
	fmt.Println(t)
	beginTime, _ := time.Parse("2006-01-02", "2023-02-01")
	endTime, _ := time.Parse("2006-01-02", "2023-02-02")
	frequency := 60 * 10
	s2 := utils.SplitTimeList(beginTime, endTime, frequency)
	for _, v := range s2 {
		fmt.Println(v)
	}
	utils.GetMysql()
}
