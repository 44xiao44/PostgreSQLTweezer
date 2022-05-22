package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slow_sql/process"
	"slow_sql/stringoperate"
	"time"
)

func main() {

	start := time.Now().UnixNano() / 1e6
	//read input params
	params := getUserInput()

	//fmt.Printf("input-->-f:%s  -tr:%s -u:%t -db:%t -dt:%t \n", params.FilePath, params.TimeRange, params.UserName, params.DataBaseName, params.Duration)

	if params.Help {
		fmt.Printf(`help info :
-h          | --help           : Get help information
-i          | --file or folder : The log file path or log folder path
-o          | --output folder  : The result output path, if not specified, outputs the result to the current folder
--startTime | --start time     : Filter start time
--endTime   | --end time       : Filter end time
--user      | --user name      : Split by username
--dbname    | --database name  : Split by database name
--duration  | --duration       : Split by duration


example ：
    PostgreSQLTweezer -i postgresql_log --dbname --duration --user


`)
		return
	}

	//log file path or log folder path
	inputFilePath, inputFilePathErr := filepath.Abs(params.FilePath)
	if inputFilePathErr != nil {
		log.Fatalf("input file error")
	}
	params.FilePath = inputFilePath

	//output folder
	if params.OutputPath == "" {
		params.OutputPath, _ = os.Getwd()
	} else {
		outputFilePath, outputFilePathErr := filepath.Abs(params.FilePath)
		if outputFilePathErr != nil {
			log.Fatalf("output file error")
		}
		params.OutputPath = outputFilePath
	}

	//Temporary folder
	tmpFilter, err := os.MkdirTemp(params.OutputPath, "tmp")
	if err != nil {
		log.Fatalln(err)
	}

	fmt.Println("waiting...")
	fmt.Printf("\n\n")

	//Preprocessing (1 format the source file or all files in the source folder, output to formatFilePath, 2 get all database names and usernames)
	formatFilePath, userNameSlice, dataBaseSlice := pre(tmpFilter, params)

	//slow sql
	doProcessSlowSQL(formatFilePath, userNameSlice, dataBaseSlice, params)

	fmt.Printf("\n\n")
	fmt.Println("waiting...")
	fmt.Printf("\n\n")

	//error sql
	doProcessErrorSQL(formatFilePath, userNameSlice, dataBaseSlice, params)
	//Delete the temporary folder
	_ = os.RemoveAll(tmpFilter)

	end := time.Now().UnixNano() / 1e6
	useTime := float64(end-start) / 1000
	fmt.Printf("\n\nThis operation took %.3f seconds \n", useTime)
}

type Params struct {
	FilePath     string // The log file path or log folder path
	OutputPath   string // Output Path
	StartTime    string // Filter by: start time
	EndTime      string // Filter by: end time
	DataBaseName bool   // Split rules: By database name
	UserName     bool   // Split rules: By user name
	Duration     bool   // Split rules: By duration(执行耗时)
	Help         bool   // Help
}

func getUserInput() *Params {
	params := Params{}
	flag.BoolVar(&params.Help, "h", false, "help")
	flag.StringVar(&params.FilePath, "i", "", "The log file path or log folder path")
	flag.StringVar(&params.FilePath, "o", "", "Output Path")
	flag.StringVar(&params.StartTime, "startTime", "startTime", "Filter by: start time e.g. '2022-05-11_10:00'")
	flag.StringVar(&params.EndTime, "endTime", "endTime", "Filter by: end time e.g. '2022-05-11_23:00'")
	flag.BoolVar(&params.UserName, "user", false, "Split rules: By user name")
	flag.BoolVar(&params.DataBaseName, "dbname", false, "Split rules: By database name")
	flag.BoolVar(&params.Duration, "duration", false, "Split rules: By duration(执行耗时)")
	flag.Parse()
	return &params
}

//pre 预处理
func pre(tmpFilter string, params *Params) (formatFilePath string, userNameSlice, dataBaseSlice *[]string) {

	//格式化文件路径
	formatFilePath = filepath.Join(tmpFilter, "tmp_format.csv")

	//格式化文件(去除多余的换行)
	process.FormatFileByBlock(params.FilePath, formatFilePath)

	//获取用户名 数组 和 数据库名数组
	if params.UserName || params.DataBaseName {
		userNameSliceTmp, dataBaseSliceTmp := process.GetDataBaseAndUser(formatFilePath)
		if params.UserName {
			userNameSlice = userNameSliceTmp
		}
		if params.DataBaseName {
			dataBaseSlice = dataBaseSliceTmp
		}
	}
	return
}

//doProcessSlowSQL 开始分析慢sql
func doProcessSlowSQL(formatFilePath string, userNameSlice, dataBaseSlice *[]string, params *Params) {

	//获取耗时分段
	var durationTimeRangeSlice []*process.SlowSQLDurationTimeRange
	if params.Duration {
		durationTimeRangeSlice = process.GetSlowSQLDurationTimeRangeSlice()
	}
	//获取开始时间，结束时间
	startTime := stringoperate.GetTime(params.StartTime)
	endTime := stringoperate.GetTime(params.EndTime)

	// 获取慢sql拆分规则 GetSlowSQLSplitInfoSlice
	slowSQLSplitInfoSlice := process.GetSplitInfoSlice(startTime, endTime, params.OutputPath, userNameSlice, dataBaseSlice, durationTimeRangeSlice)
	//根据拆分规则 一次输出所有文件
	process.SplitFileBySlowSQLRuleLazy(startTime, endTime, formatFilePath, slowSQLSplitInfoSlice)
}

//doProcessErrorSQL 开始分析错误sql
func doProcessErrorSQL(formatFilePath string, userNameSlice, dataBaseSlice *[]string, params *Params) {
	//获取开始时间，结束时间
	startTime := stringoperate.GetTime(params.StartTime)
	endTime := stringoperate.GetTime(params.EndTime)
	// 获取慢sql拆分规则 GetSlowSQLSplitInfoSlice
	errorSQLSplitInfo := process.GetErrorSQLSplitInfoSlice(startTime, endTime, params.OutputPath, userNameSlice, dataBaseSlice)

	//根据拆分规则 一次输出所有文件
	process.SplitErrorSQLFileByRuleLazy(startTime, endTime, formatFilePath, errorSQLSplitInfo)
}
