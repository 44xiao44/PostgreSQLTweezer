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
	"time"
)

const (
	ErrorSqlFolder = "ErrorSQLResult" //错误sql文件夹
)

var ErrorSqlFolderPath = ""

//ErrorSQLWriteInfo 错误sql写入信息结构体
type ErrorSQLWriteInfo struct {
	File   *os.File      //输出文件名
	Writer *bufio.Writer //输出文件writer
	LineNO int64         //写入行数
	DB     string        //数据库
	User   string        //用户
}

//ErrorSQLSplitInfo 慢SQL拆分规则 结构体
type ErrorSQLSplitInfo struct {
	UserName           string    // 用户名
	DataBaseName       string    // 数据库名称
	StartTime          time.Time // 开始时间
	EndTime            time.Time // 结束时间
	OutputFileFillName string    // 输出文件名
}

//SplitErrorSQLFileByRuleLazy 根据规则分割成多个文件
func SplitErrorSQLFileByRuleLazy(startTime, endTime time.Time, inputFilePath string, splitInfoSlice []*ErrorSQLSplitInfo) {

	// 读取文件
	csvFile, err := os.OpenFile(inputFilePath, os.O_RDONLY, 0666)
	if err != nil {
		log.Fatalf("can not open the file, err is %+v", err)
	}
	defer func() {
		_ = csvFile.Close()
	}()
	reader := bufio.NewReader(csvFile)

	writerInfoMap := make(map[string]*ErrorSQLWriteInfo)
	i := 0
	for {
		//读取1行数据
		line, err := reader.ReadString('\n')

		//如果行数据有值才处理
		if len(line) > 0 {
			recordInfo := stringoperate.GetRecordInfo(&line)
			//过滤错误sql关键词

			if recordInfo.ExecutionTime.After(startTime) &&
				recordInfo.ExecutionTime.Before(endTime) &&
				recordInfo.LogType == "ERROR" {

				for _, splitInfo := range splitInfoSlice {
					writeErrorSQLRecord(&line, &recordInfo, splitInfo, writerInfoMap)
				}
				i++
				// 每2000条数据执行一次集体 flush
				if i%2000 == 0 {
					flashErrorSQLWriter(writerInfoMap)
				}
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
	flashErrorSQLWriter(writerInfoMap)
	//close all write file 关闭所有写入文件
	closeErrorSQLWriterFile(writerInfoMap)
	//output report 输出慢sql报告
	outputErrorSQLReport(writerInfoMap)
}

//outputErrorSQLReport 输出慢sql报告
func outputErrorSQLReport(writerInfoMap map[string]*ErrorSQLWriteInfo) {
	//map写入数组，为排序准备
	i := 0
	writeInfoSlice := make([]*ErrorSQLWriteInfo, len(writerInfoMap))
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

		return false
	})

	fmt.Println("Error SQL Report :")
	fmt.Printf("\n\n")
	fmt.Printf("%-20s\t%-15s\t%-10s\n",
		"DB", "User", "NO")

	for _, writeInfo := range writeInfoSlice {
		fmt.Printf("%-20s\t%-15s\t%-10d\n",
			writeInfo.DB, writeInfo.User, writeInfo.LineNO)
	}

	fmt.Printf("\n\n")
	fmt.Printf("Detailed error SQL logs in '%s' directory", ErrorSqlFolderPath)

}

//writeErrorSQLRecord 写入记录
func writeErrorSQLRecord(line *string, recordInfo *stringoperate.RecordInfo, splitInfo *ErrorSQLSplitInfo, writerInfoMap map[string]*ErrorSQLWriteInfo) {

	//如果满足条件写入对应的文件
	if (recordInfo.UserName == splitInfo.UserName || splitInfo.UserName == "*") &&
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
			writerInfoMap[splitInfo.OutputFileFillName] = &ErrorSQLWriteInfo{
				newFile,
				writer,
				1,
				splitInfo.DataBaseName,
				splitInfo.UserName}
		} else {
			//如writerInfoMap包含输出文件 写入行数+1
			writerInfoMap[splitInfo.OutputFileFillName].LineNO += 1
		}
		writer := writerInfoMap[splitInfo.OutputFileFillName].Writer
		_, _ = writer.WriteString(*line)
	}
}

//flashWriter 循环flash所有Writer
func flashErrorSQLWriter(writerInfoMap map[string]*ErrorSQLWriteInfo) {
	for _, fileInfo := range writerInfoMap {
		_ = fileInfo.Writer.Flush()
	}
}

//closeErrorSQLWriterFile 循环关闭文件
func closeErrorSQLWriterFile(writerInfoMap map[string]*ErrorSQLWriteInfo) {
	for _, fileInfo := range writerInfoMap {
		_ = fileInfo.File.Close()
	}
}

//GetErrorSQLSplitInfoSlice 获取error sql文件分片规则切片
func GetErrorSQLSplitInfoSlice(startTime, endTime time.Time, basePath string, userNameSlice, dataBaseNameSlice *[]string) []*ErrorSQLSplitInfo {

	//如果长度为0，则设置默认值："*"
	if nil == userNameSlice || len(*userNameSlice) == 0 {
		userNameSlice = &[]string{"*"}
	}
	//如果长度为0，则设置默认值："*"
	if nil == dataBaseNameSlice || len(*dataBaseNameSlice) == 0 {
		dataBaseNameSlice = &[]string{"*"}
	}

	resultFolder := filepath.Join(basePath, ErrorSqlFolder)
	ErrorSqlFolderPath = resultFolder
	//清空存放分析结果的文件夹
	_ = os.RemoveAll(resultFolder)
	//创建存放分析结果的文件夹
	_ = os.Mkdir(resultFolder, fs.ModePerm)

	//输出文件规则
	splitInfoSlice := make([]*ErrorSQLSplitInfo, 0)
	for _, dataBaseName := range *dataBaseNameSlice {
		//输出文件路径
		outputFilePath := resultFolder
		//如果分库输出结果，按数据库名创建文件夹
		if dataBaseName != "*" {
			outputFilePath = filepath.Join(resultFolder, dataBaseName)
		}

		for _, userName := range *userNameSlice {
			//输出文件 名
			fileName := fmt.Sprintf("%s_%s%s", userName, "error", ".csv")
			//输出文件 全名（带路径）
			outputFileFillName := filepath.Join(outputFilePath, fileName)

			temporarySplitInfoItem := ErrorSQLSplitInfo{
				userName,
				dataBaseName,
				startTime,
				endTime,
				outputFileFillName,
			}
			splitInfoSlice = append(splitInfoSlice, &temporarySplitInfoItem)
		}

	}
	return splitInfoSlice
}
