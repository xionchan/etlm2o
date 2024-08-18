/*
并行迁移MySQL的数据到Oracle数据库，实现逻辑 ：
1. 函数SplitIdx : 按照主键进行分页，拆分获取主键值并放入通道c1
2. 函数FetchData : 根据通道c1的主键值，构造查询sql语句并执行，结果以接口切片的方式放入通道c2
3. 函数ConvData : 从通道c2中获取值，进行转换重组，将数据放入通道c3
4. 函数LoadData : 从通道c3中获取数据，批量插入到Oracle数据库
*/

package main

import (
	"etlm2o/common"
	"etlm2o/etldata"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"
)

func main() {
	startTime := time.Now()
	runtime.GOMAXPROCS(common.Parallel)
	// 初始化通道
	rDataChan := make(chan []interface{}, common.Parallel)
	fDataChan := make(chan []interface{}, common.Parallel)

	var wg1 sync.WaitGroup
	var wg2 sync.WaitGroup
	var wg3 sync.WaitGroup
	var wg4 sync.WaitGroup

	if !common.IsPartTab {
		keyChan := make(chan common.ColIdx, common.Parallel)
		wg1.Add(1)
		go func() {
			defer wg1.Done()
			err := etldata.SplitIdx(keyChan)
			if err != nil {
				log.Fatal(err)
			}
		}()

		for i := 0; i < common.Parallel; i++ {
			wg2.Add(1)
			go func() {
				defer wg2.Done()
				err := etldata.FetchData(keyChan, rDataChan)
				if err != nil {
					log.Fatal(err)
				}
			}()
		}

		for i := 0; i < common.Parallel; i++ {
			wg3.Add(1)
			go func() {
				defer wg3.Done()
				err := etldata.ConvData(rDataChan, fDataChan)
				if err != nil {
					log.Fatal(err)
				}
			}()
		}

		for i := 0; i < common.Parallel; i++ {
			wg4.Add(1)
			go func() {
				defer wg4.Done()
				err := etldata.LoadData(fDataChan)
				if err != nil {
					log.Fatal(err)
				}
			}()
		}

		wg1.Wait()
		close(keyChan)
		wg2.Wait()
		close(rDataChan)
		wg3.Wait()
		close(fDataChan)
		wg4.Wait()
	} else {
		keyChan := make(chan string, common.Parallel)
		wg1.Add(1)
		go func() {
			defer wg1.Done()
			err := etldata.SplitPart(keyChan)
			if err != nil {
				log.Fatal(err)
			}
		}()

		for i := 0; i < common.Parallel; i++ {
			wg2.Add(1)
			go func() {
				defer wg2.Done()
				err := etldata.FetchPartData(keyChan, rDataChan)
				if err != nil {
					log.Fatal(err)
				}
			}()
		}

		for i := 0; i < common.Parallel; i++ {
			wg3.Add(1)
			go func() {
				defer wg3.Done()
				err := etldata.ConvData(rDataChan, fDataChan)
				if err != nil {
					log.Fatal(err)
				}
			}()
		}

		for i := 0; i < common.Parallel; i++ {
			wg4.Add(1)
			go func() {
				defer wg4.Done()
				err := etldata.LoadData(fDataChan)
				if err != nil {
					log.Fatal(err)
				}
			}()
		}

		wg1.Wait()
		close(keyChan)
		wg2.Wait()
		close(rDataChan)
		wg3.Wait()
		close(fDataChan)
		wg4.Wait()
	}

	elapsedSeconds := time.Now().Sub(startTime).Seconds()
	fmt.Printf("Copy data from [%s] to [%s] finish, time : %.2fs\n", common.ST, common.TT, elapsedSeconds)
}
