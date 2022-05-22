package process

import (
	"bufio"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"slow_sql/stringoperate"
	"sort"
	"strings"
	"time"
)

const (
	SlowSqlFolder = "SlowSQLResult" //慢sql文件夹
)

var SlowSqlFolderPath = ""

//SlowSQLWriteInfo 写入信息结构体
type SlowSQLWriteInfo struct {
	File         *os.File      //输出文件名
	Writer       *bufio.Writer //输出文件writer
	LineNO       int64         //写入行数
	DB           string        //数据库
	User         string        //用户
	DurationDesc string        //耗时
}

//SlowSQLDurationTimeRange 耗时信息结构体
type SlowSQLDurationTimeRange struct {
	MinDuration float64 // 最小耗时，单位毫秒
	MaxDuration float64 // 最大耗时，单位毫秒
	Describe    string  // 耗时描述
}

//SlowSQLSplitInfo 慢SQL拆分规则 结构体
type SlowSQLSplitInfo struct {
	UserName           string    // 用户名
	DataBaseName       string    // 数据库名称
	StartTime          time.Time // 开始时间
	EndTime            time.Time // 结束时间
	MinDuration        float64   // 最小用时
	MaxDuration        float64   // 最大用时
	DurationDesc       string    // 耗时描述
	OutputFileFillName string    // 输出文件名
}

//SplitFileBySlowSQLRuleLazy 根据规则分割成多个文件
func SplitFileBySlowSQLRuleLazy(startTime, endTime time.Time, inputFilePath string, splitInfoSlice []*SlowSQLSplitInfo) {

	// 读取文件
	csvFile, err := os.OpenFile(inputFilePath, os.O_RDONLY, 0666)
	if err != nil {
		log.Fatalf("can not open the file, err is %+v", err)
	}
	defer func() {
		_ = csvFile.Close()
	}()
	reader := bufio.NewReader(csvFile)

	writerInfoMap := make(map[string]*SlowSQLWriteInfo)
	i := 0
	for {
		//读取1行数据
		line, err := reader.ReadString('\n')

		//如果行数据有值才处理
		if len(line) > 0 {
			recordInfo := stringoperate.GetRecordInfo(&line)
			//过滤慢sql关键词
			if recordInfo.ExecutionTime.After(startTime) &&
				recordInfo.ExecutionTime.Before(endTime) &&
				recordInfo.LogType == "LOG" &&
				strings.Contains(line, "duration: ") {
				for _, splitInfo := range splitInfoSlice {
					writeSlowSQLRecord(&line, &recordInfo, splitInfo, writerInfoMap)
				}
				// 每写入10000条数据执行一次集体 flush
				if i%10000 == 0 {
					flashSlowSQLWriter(writerInfoMap)
				}
				i++
			}
		}
		//文件非正常结束
		if err != nil && err != io.EOF {
			log.Fatalf("File format error, record on line:  %+v", err)
		}
		// 文件结束判断
		if err != nil && err == io.EOF {
			break
		}
	}

	//flash all writer in map 管道数据写盘
	flashSlowSQLWriter(writerInfoMap)
	//close all write file 关闭所有写入文件
	closeSlowSQLWriterFile(writerInfoMap)
	//output report 输出慢sql报告
	outputSlowSQLReport(writerInfoMap)
}

//outputSlowSQLReport 输出慢sql报告
func outputSlowSQLReport(writerInfoMap map[string]*SlowSQLWriteInfo) {
	//map写入数组，为排序准备
	i := 0
	writeInfoSlice := make([]*SlowSQLWriteInfo, len(writerInfoMap))
	for _, writeInfo := range writerInfoMap {
		writeInfoSlice[i] = writeInfo
		i++
	}

	// 排序
	sort.Slice(writeInfoSlice, func(i, j int) bool {
		if writeInfoSlice[i].DB < writeInfoSlice[j].DB {
			return true
		}
		if writeInfoSlice[i].DB == writeInfoSlice[j].DB &&
			writeInfoSlice[i].User < writeInfoSlice[j].User {
			return true
		}
		if writeInfoSlice[i].DB == writeInfoSlice[j].DB &&
			writeInfoSlice[i].User == writeInfoSlice[j].User &&
			writeInfoSlice[i].DurationDesc == writeInfoSlice[j].DurationDesc {
			return true
		}
		return false
	})

	fmt.Println("Slow SQL Report :")
	fmt.Printf("\n\n")

	fmt.Printf("%-20s\t%-15s\t%-18s\t%-10s\n",
		"DB", "User", "Duration", "NO")

	for _, writeInfo := range writeInfoSlice {
		fmt.Printf("%-20s\t%-15s\t%-18s\t%-10d\n",
			writeInfo.DB, writeInfo.User, writeInfo.DurationDesc, writeInfo.LineNO)
	}

	fmt.Printf("\n\n")
	fmt.Printf("Detailed slow SQL logs in '%s' directory", SlowSqlFolderPath)
}

//writeRecord 写入记录
func writeSlowSQLRecord(line *string, recordInfo *stringoperate.RecordInfo, splitInfo *SlowSQLSplitInfo, writerInfoMap map[string]*SlowSQLWriteInfo) {
	//如果满足条件写入对应的文件
	if recordInfo.DurationTime >= splitInfo.MinDuration && splitInfo.MaxDuration >= recordInfo.DurationTime &&
		(recordInfo.UserName == splitInfo.UserName || splitInfo.UserName == "*") &&
		(recordInfo.DataBaseName == splitInfo.DataBaseName || splitInfo.DataBaseName == "*") {

		if _, ok := writerInfoMap[splitInfo.OutputFileFillName]; !ok {
			//如writerInfoMap没有包含输出文件-->创建输出文件

			//创建全路径文件夹
			_ = os.MkdirAll(splitInfo.OutputFileFillName, fs.ModePerm)
			//删除文件夹最后一层与文件同名的文件夹
			_ = os.Remove(splitInfo.OutputFileFillName)

			newFile, err := os.OpenFile(splitInfo.OutputFileFillName, os.O_RDWR|os.O_CREATE, 0666)
			if err != nil {
				log.Fatalf("creat file error")
			}
			writer := bufio.NewWriter(newFile)
			writerInfoMap[splitInfo.OutputFileFillName] = &SlowSQLWriteInfo{
				newFile,
				writer,
				1,
				splitInfo.DataBaseName,
				splitInfo.UserName,
				splitInfo.DurationDesc}
		} else {
			//如writerInfoMap包含输出文件 写入行数+1
			writerInfoMap[splitInfo.OutputFileFillName].LineNO += 1
		}
		writer := writerInfoMap[splitInfo.OutputFileFillName].Writer
		_, _ = writer.WriteString(*line)
	}
}

//flashWriter 循环flash所有Writer
func flashSlowSQLWriter(writerInfoMap map[string]*SlowSQLWriteInfo) {
	for _, fileInfo := range writerInfoMap {
		_ = fileInfo.Writer.Flush()
	}
}

//closeWriterFile 循环关闭文件
func closeSlowSQLWriterFile(writerInfoMap map[string]*SlowSQLWriteInfo) {
	for _, fileInfo := range writerInfoMap {
		_ = fileInfo.File.Close()
	}
}

//GetDataBaseAndUser 获取全部数据库名 和 用户名
func GetDataBaseAndUser(filePath string) (userNameArray, dataBaseArray *[]string) {

	csvFile, err := os.OpenFile(filePath, os.O_RDONLY, 0666)
	if err != nil {
		log.Fatalf("can not open the file, err is %+v", err)
	}
	defer func() {
		_ = csvFile.Close()
	}()
	reader := bufio.NewReader(csvFile)

	dataBaseMap := make(map[string]bool)
	userNameMap := make(map[string]bool)

	for {
		line, err := reader.ReadString('\n')

		//文件非正常结束
		if err != nil && err != io.EOF {
			log.Fatalf("File format error, record on line:  %+v", err)
		}
		if len(line) > 0 {
			record := stringoperate.GetRecordInfo(&line)
			userNameMap[record.UserName] = true
			dataBaseMap[record.DataBaseName] = true
		}
		// 文件结束判断
		if err != nil && err == io.EOF {
			break
		}
	}
	userNameSlice := make([]string, 0)
	for key := range userNameMap {
		userNameSlice = append(userNameSlice, key)
	}
	dataBaseSlice := make([]string, 0)
	for key := range dataBaseMap {
		dataBaseSlice = append(dataBaseSlice, key)
	}
	return &userNameSlice, &dataBaseSlice
}

//GetSplitInfoSlice 获取文件分片规则切片
func GetSplitInfoSlice(startTime, endTime time.Time, basePath string, userNameSlice, dataBaseNameSlice *[]string, durationTimeRangeSlice []*SlowSQLDurationTimeRange) []*SlowSQLSplitInfo {

	//如果没传入时长分段，设置默认分段
	if nil == durationTimeRangeSlice || len(durationTimeRangeSlice) == 0 {
		durationTimeRangeSlice = []*SlowSQLDurationTimeRange{
			{0, 900000000000, "不限时长"}}
	}
	//如果长度为0，则设置默认值："*"
	if nil == userNameSlice || len(*userNameSlice) == 0 {
		userNameSlice = &[]string{"*"}
	}
	//如果长度为0，则设置默认值："*"
	if nil == dataBaseNameSlice || len(*dataBaseNameSlice) == 0 {
		dataBaseNameSlice = &[]string{"*"}
	}

	resultFolder := filepath.Join(basePath, SlowSqlFolder)
	SlowSqlFolderPath = resultFolder
	//清空存放分析结果的文件夹
	_ = os.RemoveAll(resultFolder)
	//创建存放分析结果的文件夹
	_ = os.Mkdir(resultFolder, fs.ModePerm)

	//输出文件规则
	splitInfoSlice := make([]*SlowSQLSplitInfo, 0)
	for _, dataBaseName := range *dataBaseNameSlice {
		//输出文件路径
		outputFilePath := resultFolder
		//如果分库输出结果，按数据库名创建文件夹
		if dataBaseName != "*" {
			outputFilePath = filepath.Join(resultFolder, dataBaseName)
			//_ = os.Mkdir(outputFilePath, fs.ModePerm)
		}

		for _, durationTimeRange := range durationTimeRangeSlice {
			for _, userName := range *userNameSlice {
				//输出文件 名
				fileName := fmt.Sprintf("%s_%s%s", userName, durationTimeRange.Describe, ".csv")
				//输出文件 全名（带路径）
				outputFileFillName := filepath.Join(outputFilePath, fileName)

				temporarySplitInfoItem := SlowSQLSplitInfo{
					userName,
					dataBaseName,
					startTime,
					endTime,
					durationTimeRange.MinDuration,
					durationTimeRange.MaxDuration,
					durationTimeRange.Describe,
					outputFileFillName,
				}
				splitInfoSlice = append(splitInfoSlice, &temporarySplitInfoItem)
			}
		}
	}
	return splitInfoSlice
}

//GetSlowSQLDurationTimeRangeSlice 慢SQL常用时间范围
func GetSlowSQLDurationTimeRangeSlice() []*SlowSQLDurationTimeRange {
	return []*SlowSQLDurationTimeRange{
		{1000, 2000, "1-2Second"},
		{2000, 3000, "2-3Second"},
		{3000, 5000, "3-5Second"},
		{5000, 7000, "5-7Second"},
		{7000, 10000, "7-10Second"},
		{10000, 60000, "10-60Second"},
		{60000, 300000, "1-5Minutes"},
		{300000, 600000, "5-10Minutes"},
		{600000, 60480000, "MoreThan10Minutes"}, //10分-7天
	}
}
