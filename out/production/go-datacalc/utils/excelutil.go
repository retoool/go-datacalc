package utils

import (
	"fmt"
	"github.com/360EntSecGroup-Skylar/excelize"
)

func ReadExcel() {
	// 打开 Excel 文件
	f, err := excelize.OpenFile("example.xlsx")
	if err != nil {
		fmt.Println(err)
		return
	}
	// 读取第一个工作表中的所有单元格
	rows := f.GetRows("Sheet1")
	// 输出所有单元格的值
	for _, row := range rows {
		for _, col := range row {
			fmt.Print(col, "\t")
		}
		fmt.Println()
	}
}
