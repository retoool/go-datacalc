package utils

import (
	"fmt"

	"github.com/gogf/gf/v2/frame/g"
)

type Config struct {
	Database struct {
		Default struct {
			Link    string
			Charset string
		}
	}
}

func main() {
	link := MysqlLink.String()
	charset := MysqlCharset.String()
	db := g.DB().SetDSN(link + "&charset=" + charset)
	result, err := db.Table("table1").All()
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(result.ToList())
}
