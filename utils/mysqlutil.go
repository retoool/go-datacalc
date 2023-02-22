package utils

import (
	"database/sql"
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
	defer db.Close()
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
	defer db.Close()
	_, err = db.Exec(execsql, args...)
	if err != nil {
		return err
	}
	return nil
}
