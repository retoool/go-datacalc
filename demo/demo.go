package demo

import "sync"

type test struct {
	a int
	b int
	c int
}

var instanceSqlData *test
var onceSqlData sync.Once

func Getdemo() *test {
	onceSqlData.Do(func() {
		instanceSqlData = &test{}
	})
	return instanceSqlData
}

func (t *test) Getvalue() int {
	return t.a
}

func (t *test) Setvalue(i int) {
	t.a = i
}

func (t *test) init() {
	t.a = 1
}
