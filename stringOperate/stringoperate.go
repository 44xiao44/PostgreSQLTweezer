package stringoperate

import (
	"encoding/csv"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var LineBeginningRegexp *regexp.Regexp //获取开始日期"_2022-05-06 00:00:09.314" 的正则
var RegexpGetDuration *regexp.Regexp   //获取执行时间正则
const formatLayout = "2006-01-02 15:04:05"

func init() {
	LineBeginningRegexp, _ = regexp.Compile(`_\d{4}-\d{2}-\d{2} `)
	RegexpGetDuration, _ = regexp.Compile(`duration: \d{1,9}.\d{1,4} ms`)
}

//FormatString 格式化字符串
func FormatString(targetStr, suffixStr string) string {

	//"目标字符串" 所有换行替换为下划线(字符串长度不变)
	targetStr = strings.ReplaceAll(targetStr, "\n", "_")
	//临时字符串("目标字符串"+后缀字符串)
	tmpStr := targetStr + suffixStr

	//获取行开头数组, 格式："_2022-05-06 00:00:09.314"
	lineBeginningArray := LineBeginningRegexp.FindAllString(tmpStr, -1)

	// 去重
	newLineBeginningArray := deduplication(&lineBeginningArray)

	//循环替换行开头字符串的 "_" 为换行符"\n"
	for _, lineBeginning := range *newLineBeginningArray {
		// 头部的"_"替换成 "换行符"
		tmpStr = strings.ReplaceAll(tmpStr, lineBeginning, "\n"+lineBeginning[1:])
	}

	//从临时字符串中截取出"目标字符串"
	return tmpStr[:len(targetStr)]
}

//deduplication 字符串数组去重
func deduplication(input *[]string) *[]string {
	tmpMap := make(map[string]bool)
	for _, key := range *input {
		tmpMap[key] = true
	}
	var output []string
	for key := range tmpMap {
		output = append(output, key)
	}
	return &output
}

//GetTime 根据时间格式"2022-05-10 01:00"的字符串 获取时间"startTime":2000年之前, "endTime":2000年之后
func GetTime(inputStr string) time.Time {

	if inputStr == "startTime" {
		return time.Now().AddDate(-2000, 0, 0)
	} else if inputStr == "endTime" {
		return time.Now().AddDate(2000, 0, 0)
	}

	if len(inputStr) != 16 {
		log.Fatal("GetTime format error,input value like '2022-05-10 01:00'")
	}
	dateStr := inputStr[0:10]
	timeStr := dateStr + " " + inputStr[11:16] + ":00"
	resultTime, err1 := time.Parse(formatLayout, timeStr)

	if err1 != nil {
		log.Fatal("timeRangeStr format error")
	}
	return resultTime
}

//RecordInfo 日志记录信息
type RecordInfo struct {
	DurationTime  float64   //执行耗时
	UserName      string    //用户名
	DataBaseName  string    //数据库名
	SQLType       string    //sql类型（select，insert等）
	LogType       string    //数据库日志类型
	ExecutionTime time.Time //执行时间点
}

//GetRecordInfo 获取 日志记录信息
func GetRecordInfo(record *string) (recordInfo RecordInfo) {

	if len(*record) == 0 {
		recordInfo.DurationTime = 0
		recordInfo.UserName = ""
		recordInfo.DataBaseName = ""
		recordInfo.ExecutionTime = time.Now()
		return
	}

	//用正则获取 关键内容"duration: 1090.150 ms"
	durationStr := RegexpGetDuration.FindString(*record)
	if len(durationStr) > 0 {
		//获取中间的数字
		durationStr = durationStr[10 : len(durationStr)-3]
		// 转为float64类型
		durationTime, _ := strconv.ParseFloat(durationStr, 64)
		recordInfo.DurationTime = durationTime
	}

	//按csv形式读取
	csvRecord, _ := csv.NewReader(strings.NewReader(*record)).Read()
	// 判断长度
	if len(csvRecord) > 2 {
		executionTimeStr := csvRecord[0][:19]
		recordInfo.UserName = csvRecord[1]
		recordInfo.DataBaseName = csvRecord[2]
		recordInfo.SQLType = csvRecord[7]
		recordInfo.LogType = csvRecord[11]
		//执行时间格式化成时间类型
		executionTime, timeErr := time.Parse(formatLayout, executionTimeStr)
		if timeErr == nil {
			recordInfo.ExecutionTime = executionTime
		}
	}
	return
}
