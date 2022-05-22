package test

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"slow_sql/stringoperate"
	"strings"
	"testing"
)

var LineBeginningRegexp *regexp.Regexp //获取开始日期"_2022-05-06 00:00:09.314" 的正则

func init() {
	//LineBeginningRegexp, _ = regexp.Compile(`_\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{3}`)
	LineBeginningRegexp, _ = regexp.Compile(`_\d{4}-\d{2}-\d{2} `)
}

//字符串数组去重
func TestGetTimeRange(t *testing.T) {
	//开始时间结束时间
	startTime, endTime := stringoperate.GetTimeRange("*")
	fmt.Println(startTime)
	fmt.Println(endTime)
}

//字符串数组去重
func TestDeduplication(t *testing.T) {
	testInput := []string{"a", "a", "a", "d", "e", "f", "g", "h"}
	result := stringoperate.Deduplication(&testInput)
	fmt.Println(result)
}

//TestGetDurationTime 测试获取执行时间
//func TestGetDurationTime(t *testing.T){
//	str := "1923duration: 1090.150 ms819023"
//	result := stringOperate.GetDurationTime(str)
//	fmt.Printf("%.1f", result)
//}

func TestStr(t *testing.T) {
	str := `stgreSQL JDBC Driver"_2022-05-07 00:00:09.314 CST,"admin","ess",28921,"172.25.145.27:40690",62753ef8stgreSQL JDBC Driver"_2022-05-06 00:00:09.314 CST,"admin","ess",28921,"172.25.145.27:40690",62753ef8C Driver"_2022-05-06 00:00:09.314 CST,"admin","ess`

	lineBeginningArray := LineBeginningRegexp.FindAllString(str, -1)

	fmt.Println(lineBeginningArray)
	for _, lineBeginning := range lineBeginningArray {
		fmt.Println(lineBeginning)
	}
	//formatFile("test1.txt", "test2.txt")
}

func formatFile(filePath, newFilePath string) {
	//原文件
	file, err := os.OpenFile(filePath, os.O_RDWR, 0666)
	if err != nil {
		log.Fatalf("can not open the file, err is %+v", err)
	}
	defer func() {
		_ = file.Close()
	}()
	reader := bufio.NewReader(file)

	//新文件
	_ = os.Remove(newFilePath)
	newFile, err := os.OpenFile(newFilePath, os.O_RDWR|os.O_CREATE, 0666)
	defer func() {
		_ = newFile.Close()
	}()
	writer := bufio.NewWriter(newFile)

	// 每次读取 1024 个字节
	buf := make([]byte, 20)
	lastBufString := ""
	for {
		n, err := reader.Read(buf)
		if err != nil && err != io.EOF {
			panic(err)
		}
		if n == 0 {
			break
		}
		//新读取的字符串
		newLineStr := string(buf[:n])

		//如果上次读取的部分不为空
		if len(lastBufString) != 0 {
			lastBufString = formatString(lastBufString, newLineStr)
			_, _ = writer.WriteString(lastBufString)
		}

		lastBufString = newLineStr
	}

	lastBufString = formatString(lastBufString, "")
	_, _ = writer.WriteString(lastBufString)

	_ = writer.Flush()
}

func formatString(targetStr, suffixStr string) string {
	//"目标字符串" 所有换行替换为下划线(字符串长度不变)
	targetStr = strings.ReplaceAll(targetStr, "\n", "_")
	//临时字符串("目标字符串"+后缀字符串)
	tmpStr := targetStr + suffixStr
	//临时字符串还原日期前面的换行
	tmpStr = strings.ReplaceAll(tmpStr, "_2022-05-07 ", "\n2022-05-07 ")
	//从临时字符串中截取出"目标字符串"
	return tmpStr[:len(targetStr)]
}
