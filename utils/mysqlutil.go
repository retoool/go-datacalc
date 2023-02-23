package utils

import (
	"database/sql"
	"fmt"
	"github.com/go-sql-driver/mysql"
)

func ConnectMysql() (*sql.DB, error) {
	cfg := mysql.Config{
		User:                 MysqlUser,
		Passwd:               MysqlPassword,
		Net:                  "tcp",
		Addr:                 MysqlAddr,
		DBName:               MysqlDatabase,
		AllowNativePasswords: true,
	}
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, err
	}
	return db, err

}
func QueryMysql(querysql string, args ...any) (*sql.Rows, error) {
	db, err := ConnectMysql()
	if err != nil {
		return nil, err
	}
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			fmt.Println(err)
		}
	}(db)
	rows, err := db.Query(querysql, args...)
	if err != nil {
		return nil, err
	}
	return rows, err
}
func ExecMysql(execsql string, args ...interface{}) error {
	db, err := ConnectMysql()
	if err != nil {
		return err
	}
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			fmt.Println(err)
		}
	}(db)
	_, err = db.Exec(execsql, args...)
	if err != nil {
		return err
	}
	return nil
}
