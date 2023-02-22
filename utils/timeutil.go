package utils

import (
	"fmt"
	"time"
)

// 获取当前时间
func GetNowTime() time.Time {
	return time.Now()
}

func StrToTime(timestr string) time.Time {
	layout := "2006-01-02 15:04:05" // 时间字符串的格式
	t, err := time.Parse(layout, timestr)
	if err != nil {
		panic(err)
	}
	return t
}
func TimeToStr(t time.Time) string {
	layout := "2006-01-02 15:04:05" // 时间字符串的格式
	timestr := t.Format(layout)
	return timestr
}
func TimeInit() (string, string) {
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		panic(err)
	}
	today := time.Now().In(loc)
	todayTime := time.Date(today.Year(), today.Month(), today.Day(), 0, 0, 0, 0, loc)
	todayTimeUnix := todayTime.Unix()
	todayTimeStr := time.Unix(todayTimeUnix, 0).Format("2006-01-02 15:04:05")

	yesterday := today.Add(-24 * time.Hour)
	yesterdayTime := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, loc)
	yesterdayTimeUnix := yesterdayTime.Unix()
	yesterdayTimeStr := time.Unix(yesterdayTimeUnix, 0).Format("2006-01-02 15:04:05")

	fmt.Println(yesterdayTimeStr + " to " + todayTimeStr)
	return yesterdayTimeStr, todayTimeStr
}

// 时间切片
func SplitTimeRanges(from_time time.Time, to_time time.Time, frequency int) [][]string {
	time_range := make([]time.Time, 0)
	for from_time.Before(to_time) {
		time_range = append(time_range, from_time)
		from_time = from_time.Add(time.Duration(frequency) * time.Second)
	}
	if !from_time.Equal(to_time) {
		time_range = append(time_range, to_time)
	}
	fmt.Println(time_range)
	time_ranges := make([][]string, 0)
	for _, item := range time_range {
		f_time := item.Format("2006-01-02 15:04:05")
		t_time := item.Add(time.Duration(frequency) * time.Second).Format("2006-01-02 15:04:05")
		if t_time >= to_time.Format("2006-01-02 15:04:05") {
			t_time = to_time.Format("2006-01-02 15:04:05")
			time_ranges = append(time_ranges, []string{f_time, t_time})
			break
		}
		time_ranges = append(time_ranges, []string{f_time, t_time})
	}
	return time_ranges
}

// 时间序列
func SplitTimeList(from_time time.Time, to_time time.Time, frequency int) []string {
	time_range := make([]time.Time, 0)
	for from_time.Before(to_time) {
		time_range = append(time_range, from_time)
		from_time = from_time.Add(time.Duration(frequency) * time.Second)
	}
	if !from_time.Equal(to_time) {
		time_range = append(time_range, to_time)
	}

	time_range_str := make([]string, 0)
	for _, item := range time_range {
		item := item.Format("2006-01-02 15:04:05")
		time_range_str = append(time_range_str, item)
	}
	return time_range_str
}
