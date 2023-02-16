package utils

import (
	"fmt"
	"time"
)

// 获取当前时间
func GetNowTime() time.Time {
	return time.Now()
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
