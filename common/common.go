package common

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"regexp"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/godror/godror"
)

// 定义拆分列的区间值
type ColIdx struct {
	BeginValue string
	EndValue   string
}

var (
	MysDsn    string   // MySQL的连接信息
	OraDsn    string   // Oracle的连接信息
	Parallel  int      // 并行度，数据库层
	SSchema   string   // MySQL的数据库名字
	STable    string   // MySQL的表名字
	TSchema   string   // Oracle的数据库名字
	TTable    string   // Oracle的表名字
	ST        string   // MySQL的数据库加表名 ：db.table
	TT        string   // Oracle的用户加表名 ：user.table
	FetchSize int      // 每个分片的大小，默认是1024
	ColLen    int      // 列长度
	ColList   []string // 列名字的切片
	ByteCol   []int    // 二进制数据列序号
	IdxCol    string   // 拆分键
	IsPartTab bool     // 是否是分区表
)

func init() {
	// 初始化命令行参数
	mysqlUser := flag.String("su", "", "force, mysql username")                          // MySQL用户名字
	mysqlPass := flag.String("sp", "", "force, mysql password, enclose in single quota") // MySQL用户密码
	mysqlDsn := flag.String("sdsn", "", "force, mysql connection info, example : ip:port/dbname")
	oracleUser := flag.String("tu", "", "force, oracle username")
	oraclePass := flag.String("tp", "", "force, oracle password, enclose in single quota")
	oracleDsn := flag.String("tdsn", "", "force, oracle connection info, example : ip:port/service_name")
	parallel := flag.Int("p", 1, "database parallel, max 16")
	sourceTable := flag.String("s", "", "force, mysql table name. db.table")
	targetTable := flag.String("t", "", "oracle table. user.table, default dbname.sourceTable")
	fetchSize := flag.Int("f", 1, "fetchsize")

	flag.Parse()

	// 检查强制参数
	requireArgs := []*string{mysqlUser, mysqlPass, mysqlDsn, oracleUser, oraclePass, oracleDsn, sourceTable}
	for _, param := range requireArgs {
		if *param == "" {
			flag.Usage()
			log.Fatal("Error : please input force args")
		}
	}

	// 校验dsn格式
	checkDsn := []*string{mysqlDsn, oracleDsn}
	for _, param := range checkDsn {
		if !validateDsn(*param) {
			flag.Usage()
			log.Fatal("Error : Incorrent formatting ", *param)
		}
	}

	// 拼接Oracle的连接串
	OraDsn = `user="` + *oracleUser + `" password="` + *oraclePass + `" connectString="(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=` +
		strings.Split(*oracleDsn, ":")[0] + `)(PORT=` + strings.Split(strings.Split(*oracleDsn, ":")[1], "/")[0] + `))(CONNECT_DATA=(SERVICE_NAME=` +
		strings.Split(*oracleDsn, "/")[1] + `)))" heterogeneousPool=false poolIncrement=1 poolMaxSessions=1000 poolMinSessions=1 poolSessionMaxLifetime=1h0m0s
		poolSessionTimeout=5m0s  standaloneConnection=false poolWaitTimeout=30s timezone="+08:00"`

	// 拼接MySQL的连接串
	MysDsn = *mysqlUser + ":" + *mysqlPass + "@tcp(" + strings.Split(*mysqlDsn, "/")[0] + ")/" + strings.Split(*mysqlDsn, "/")[1] + "?parseTime=true&loc=Local"

	// 并行度
	if *parallel > 16 {
		Parallel = 16
	} else if *parallel < 1 {
		Parallel = 1
	} else {
		Parallel = *parallel
	}

	// MySQL的表信息构造
	if strings.Contains(*sourceTable, ".") {
		SSchema = strings.ToUpper(strings.Split(*sourceTable, ".")[0])
		STable = strings.ToUpper(strings.Split(*sourceTable, ".")[1])
	} else {
		SSchema = ""
		STable = strings.ToUpper(*sourceTable)
	}

	if SSchema == "" {
		ST = STable
	} else {
		ST = SSchema + "." + STable
	}

	// Oracle的表信息构造
	if *targetTable == "" {
		TSchema = strings.ToUpper(*oracleUser)
		TTable = strings.ToUpper(STable)
	} else {
		if strings.Contains(*targetTable, ".") {
			TSchema = strings.ToUpper(strings.Split(*targetTable, ".")[0])
			TTable = strings.ToUpper(strings.Split(*targetTable, ".")[1])
		} else {
			TSchema = strings.ToUpper(*oracleUser)
			TTable = strings.ToUpper(*targetTable)
		}
	}

	TT = TSchema + "." + TTable

	// 打印迁移任务信息
	fmt.Printf("Copy data from %s[%s] to %s[%s] !\n", *oracleDsn, ST, *oracleDsn, TT)

	// 目标表必须是空表
	oradb, err := CreateDbConn("oracle", OraDsn)
	if err != nil {
		log.Fatal(err)
	}

	var dataFlag int
	checkDataSql := "select count(*) from (select * from " + TT + " where rownum < 2)"

	err = oradb.QueryRow(checkDataSql).Scan(&dataFlag)
	if err != nil {
		log.Fatal("Failed to query Oracle database : ", err)
	}
	defer oradb.Close()

	if dataFlag > 0 {
		log.Fatal("Target Table is not empty!")
	}

	// 获取表的列信息
	colInfoSql := "select rownum, a.* from (select column_name, data_type from all_tab_cols where column_name not like 'SYS\\_%$' and owner = '" +
		TSchema + "' and table_name = '" + TTable + "' order by column_name) a"

	crows, err := oradb.Query(colInfoSql)
	if err != nil {
		log.Fatal("Failed to get column information from source database : ", err)
	}

	for crows.Next() {
		var colnum int
		var colname string
		var coltype string
		err := crows.Scan(&colnum, &colname, &coltype)
		if err != nil {
			log.Fatal("Failed to fetch column information from source database :", err)
		}
		ColList = append(ColList, colname)
		if coltype == "BLOB" {
			ByteCol = append(ByteCol, colnum)
		}
	}

	ColLen = len(ColList)

	// 获取fetchsize
	if *fetchSize > 1 {
		FetchSize = *fetchSize
	} else {
		FetchSize = 1
	}

	mysdb, err := CreateDbConn("mysql", MysDsn)
	if err != nil {
		log.Fatal(err)
	}
	defer mysdb.Close()

	// 判断是否是分区表
	getPartTabSql := "select partition_name from INFORMATION_SCHEMA.PARTITIONS where TABLE_SCHEMA = '" + SSchema + "' and TABLE_NAME = '" +
		STable + "' limit 1"
	var valueStr sql.NullString

	err = mysdb.QueryRow(getPartTabSql).Scan(&valueStr)
	if err != nil {
		log.Fatal(err)
	}

	if valueStr.Valid {
		IsPartTab = true
	} else {
		IsPartTab = false
	}

	if !IsPartTab {
		datarow := make([]interface{}, 15)
		for i := 0; i < 15; i++ {
			datarow[i] = new(interface{})
		}

		getIdxCol := "show index from " + ST + " where key_name = 'PRIMARY' and seq_in_index = 1"
		err = mysdb.QueryRow(getIdxCol).Scan(datarow...)
		if err != nil {
			log.Fatal(err)
		}

		for i, data := range datarow {
			if i == 4 {
				val := *data.(*interface{})
				IdxCol = string(val.([]byte))
			}
		}
	}
}

// 校验dsn格式的函数
func validateDsn(input string) bool {
	pattern := `^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d+\/[a-zA-Z0-9_]+$`
	regex := regexp.MustCompile(pattern)
	return regex.MatchString(input)
}

// 创建数据库的连接
func CreateDbConn(dbtype, dsn string) (*sql.DB, error) {
	var err error
	var db *sql.DB
	if dbtype == "oracle" {
		db, err = sql.Open("godror", dsn)
	} else {
		db, err = sql.Open("mysql", dsn)
	}

	if err != nil {
		return nil, fmt.Errorf("Failed to connect to database : %w", err)
	}

	return db, nil
}
