package utils

import (
	"math"
	"strconv"
)

func StrToInt(str string) (int, error) {
	i, err := strconv.Atoi(str)
	if err != nil {
		return 0, err
	}
	return i, nil
}

func IntToStr(i int) string {
	s := strconv.Itoa(i)
	return s
}

func StrToFloat(str string) (float64, error) {
	float, err := strconv.ParseFloat(str, 64)
	if err != nil {
		return 0.0, err
	}
	return float, nil
}

func FloatToStr(f float64, prec int) string {
	s := strconv.FormatFloat(f, 'f', prec, 64)
	return s
}

func Round(f float64, n int) float64 {
	scale := math.Pow(10, float64(n))
	return math.Round(f*scale) / scale
}

func FindNearestIndex(arr []float64, val float64) int {
	nearestIdx := 0
	nearestDiff := math.Abs(arr[0] - val)
	for i := 1; i < len(arr); i++ {
		diff := math.Abs(arr[i] - val)
		if diff < nearestDiff {
			nearestIdx = i
			nearestDiff = diff
		}
	}
	return nearestIdx
}
