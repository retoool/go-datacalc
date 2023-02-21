package utils

import (
	"database/sql"
	"fmt"

	"github.com/go-sql-driver/mysql"
)

func GetMysql() {
	cfg := mysql.Config{
		User:   "root",
		Passwd: "admin",
		Net:    "tcp",
		Addr:   "127.0.0.1:3306",
		DBName: "t_user",
	}
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		panic(err.Error())
	}
	rows, err := db.Query("select * from t_user where id = ?", "111")
	if err != nil {
		panic(err.Error())
	}
	var id string
	var name string
	var age int
	for rows.Next() {
		err = rows.Scan(&id, &name, &age)
		if err != nil {
			panic(err.Error())
		}
		fmt.Println(id, name, age)
	}
	defer db.Close()

}
