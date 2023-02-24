package utils

import (
	"fmt"
	"strconv"
)

func StrToInt(str string) int {
	i, err := strconv.Atoi(str)
	if err != nil {
		fmt.Println(err)
	}
}
