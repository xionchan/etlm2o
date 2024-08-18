package etldata

import (
	"database/sql"
	"etlm2o/common"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"
)

func SplitIdx(idxChan chan common.ColIdx) error {
	db, err := common.CreateDbConn("mysql", common.MysDsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	firstQuery := "select " + common.IdxCol + " from " + common.STable + " order by " + common.IdxCol + " limit " + strconv.Itoa(common.FetchSize) + ",1"
	nextQuery := "select " + common.IdxCol + " from " + common.STable + " where " + common.IdxCol + " > ?" + " order by " + common.IdxCol + " limit " + strconv.Itoa(common.FetchSize) + ",1"

	// 只prepare next的sql语句
	stmt, err := db.Prepare(nextQuery)
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	skipnum := 0 // 计数器
	var idxCurValue string
	var idxPreValue string

	for {
		if skipnum == 0 {
			err := db.QueryRow(firstQuery).Scan(&idxCurValue)
			if err != nil {
				log.Fatal("query first index sql fail", err)
			}
			// 放入第一行索引数据
			idxChan <- common.ColIdx{BeginValue: "", EndValue: idxCurValue}
			idxPreValue = idxCurValue
		} else {
			err = stmt.QueryRow(idxPreValue).Scan(&idxCurValue)
			if err != nil {
				if err == sql.ErrNoRows {
					idxChan <- common.ColIdx{BeginValue: idxPreValue, EndValue: ""} // 最后一行索引数据
					return nil
				} else {
					log.Fatal("query next index sql fail", err)
				}
			}
			// 放入后面的索引数据
			idxChan <- common.ColIdx{BeginValue: idxPreValue, EndValue: idxCurValue}
			idxPreValue = idxCurValue
		}
		skipnum++
	}
	return nil
}

func SplitPart(partChan chan string) error {
	db, err := common.CreateDbConn("mysql", common.MysDsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	getPartNameSql := "select partition_name from INFORMATION_SCHEMA.PARTITIONS where TABLE_SCHEMA = '" + common.SSchema + "' and TABLE_NAME = '" + common.STable + "'"

	rows, err := db.Query(getPartNameSql)
	if err != nil {
		log.Fatal(err)
	}

	for rows.Next() {
		var partName string

		err = rows.Scan(&partName)
		if err != nil {
			log.Fatal(err)
		}

		partChan <- partName
	}
	return nil
}

func FetchData(idxChan chan common.ColIdx, rDataChan chan []interface{}) error {
	db, err := common.CreateDbConn("mysql", common.MysDsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	firstQuery := "select " + strings.Join(common.ColList, ",") + " from " + common.ST + " where " + common.IdxCol + " <= ?"
	endQuery := "select " + strings.Join(common.ColList, ",") + " from " + common.ST + " where " + common.IdxCol + " > ?"
	nextQuery := "select " + strings.Join(common.ColList, ",") + " from " + common.ST + " where " + common.IdxCol + " <= ? and " + common.IdxCol + " > ?"

	// 只prepare中间的sql语句
	stmt, err := db.Prepare(nextQuery)
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	// 存放查询结果集
	var results []interface{}

	rowcount := 0 // 行计数器

	for idxValue := range idxChan {
		var rawrows *sql.Rows
		var err error
		if idxValue.BeginValue == "" {
			rawrows, err = db.Query(firstQuery, idxValue.EndValue)
			if err != nil {
				log.Fatal(err)
			}
		} else if idxValue.EndValue == "" {
			rawrows, err = db.Query(endQuery, idxValue.BeginValue)
			if err != nil {
				log.Fatal(err)
			}
		} else {
			rawrows, err = db.Query(nextQuery, idxValue.EndValue, idxValue.BeginValue)
			if err != nil {
				log.Fatal(err)
			}
		}
		defer rawrows.Close()

		// 将查询结果放入通道rDataChan
		for rawrows.Next() {
			rowcount++
			onerow := make([]interface{}, common.ColLen)
			for i := 0; i < common.ColLen; i++ {
				onerow[i] = new(interface{})
			}
			err := rawrows.Scan(onerow...)
			if err != nil {
				log.Fatal(err)
			}

			for _, onevalue := range onerow {
				results = append(results, onevalue)
			}

			if rowcount == common.FetchSize {
				rDataChan <- results
				results = nil
				rowcount = 0
			}
		}
	}

	// 处理最后一批数据
	if len(results) > 0 {
		rDataChan <- results
	}
	return nil
}

func FetchPartData(partChan chan string, rDataChan chan []interface{}) error {
	db, err := common.CreateDbConn("mysql", common.MysDsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	queryStr := "select " + strings.Join(common.ColList, ",") + " from " + common.ST + " partition ({partname})"

	var results []interface{}

	rowcount := 0 // 行计数器

	for partName := range partChan {
		var rawrows *sql.Rows
		var err error
		querySql := strings.Replace(queryStr, "{partname}", partName, -1)
		rawrows, err = db.Query(querySql)

		if err != nil {
			log.Fatal(err)
		}
		defer rawrows.Close()

		// 将查询结果放入通道rDataChan
		for rawrows.Next() {
			rowcount++
			onerow := make([]interface{}, common.ColLen)
			for i := 0; i < common.ColLen; i++ {
				onerow[i] = new(interface{})
			}
			err := rawrows.Scan(onerow...)
			if err != nil {
				log.Fatal(err)
			}

			for _, onevalue := range onerow {
				results = append(results, onevalue)
			}

			if rowcount == common.FetchSize {
				rDataChan <- results
				results = nil
				rowcount = 0
			}
		}
	}

	// 处理最后一批数据
	if len(results) > 0 {
		rDataChan <- results
	}
	return nil
}

func ConvData(rDataChan chan []interface{}, fDataChan chan []interface{}) error {
	for rowdata := range rDataChan {
		var datav []interface{}
		for i := 0; i < common.ColLen; i++ {
			var astr []string    // 存放字符串
			var atim []time.Time // 存放时间
			var abyt [][]byte    // 存放二进制数据
			var aint []int64
			var afot []float64

			// 判断是否是二进制数据
			blobFlag := false
			for _, num := range common.ByteCol {
				if i == num-1 {
					blobFlag = true
					break
				}
			}

			if blobFlag {
				for j := i; j < len(rowdata); j += common.ColLen {
					val := *rowdata[j].(*interface{})
					switch val.(type) {
					case []uint8:
						abyt = append(abyt, val.([]uint8))
					}
				}
			} else {
				for j := i; j < len(rowdata); j += common.ColLen {
					val := *rowdata[j].(*interface{})
					switch val.(type) {
					case time.Time:
						atim = append(atim, val.(time.Time))
					case []uint8:
						astr = append(astr, string(val.([]uint8)))
					case int64:
						aint = append(aint, val.(int64))
					case float64:
						afot = append(afot, val.(float64))
					case nil:
						astr = append(astr, "")
					}
				}
			}

			var a interface{}
			if len(astr) > 0 {
				a = astr
			}

			if len(atim) > 0 {
				a = atim
			}

			if len(abyt) > 0 {
				a = abyt
			}

			if len(aint) > 0 {
				a = aint
			}

			if len(afot) > 0 {
				a = afot
			}

			datav = append(datav, a)
		}
		fDataChan <- datav
	}
	return nil
}

func LoadData(fDataChan chan []interface{}) error {
	db, err := common.CreateDbConn("oracle", common.OraDsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// sql语句的绑定变量部分构造
	valueBindStr := ""
	for i := 0; i < common.ColLen; i++ {
		valueBindStr = valueBindStr + ":" + strconv.Itoa(i) + ","
	}
	insertSql := "insert into " + common.TT + "(" + strings.Join(common.ColList, ",") + ") values (" + valueBindStr[:len(valueBindStr)-1] + ")"

	stmt, err := db.Prepare(insertSql)
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	for fdata := range fDataChan {
		_, err = stmt.Exec(fdata...)
		if err != nil {
			fmt.Println(strings.Join(common.ColList, ","))
			log.Fatal(err)
		}
	}
	return nil
}
