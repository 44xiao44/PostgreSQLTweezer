package process

import (
	"bufio"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"slow_sql/stringoperate"
)

//FormatFileByBlock 按块读取，格式化文件
func FormatFileByBlock(filePath, newFilePath string) {

	inputFileSlice, err := getCvsFileSlice(filePath, ".csv")
	if err != nil {
		log.Fatalf("can not open the file, err is %+v", err)
	}

	//创建输出文件
	newFile, err := os.OpenFile(newFilePath, os.O_RDWR|os.O_CREATE, 0666)
	defer func() {
		_ = newFile.Close()
	}()

	writer := bufio.NewWriter(newFile)

	//循环源文件列表 读取源文件格式化后写入新文件
	for _, inputFilePath := range inputFileSlice {
		readFileAndWrite(inputFilePath, writer)
		_ = writer.Flush()
	}

	_ = writer.Flush()
}

// 读文件&写入
func readFileAndWrite(inputFilePath string, writer *bufio.Writer) {

	//需要读取的文件
	readFile, err := os.OpenFile(inputFilePath, os.O_RDWR, 0666)
	if err != nil {
		log.Fatalf("can not open the file, err is %+v", err)
	}
	defer func() {
		_ = readFile.Close()
	}()
	reader := bufio.NewReader(readFile)

	// 每次读取 1024 个字节
	buf := make([]byte, 1024)
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
			lastBufString = stringoperate.FormatString(lastBufString, newLineStr)
			_, _ = writer.WriteString(lastBufString)
		}

		lastBufString = newLineStr
	}
	//最后一段数据处理
	lastBufString = stringoperate.FormatString(lastBufString, "")
	// 每个文件结束后，给末尾添加换行
	lastBufString += "\n"
	_, _ = writer.WriteString(lastBufString)
}

//getFileSliceByType 获取文件列表
func getCvsFileSlice(inputFilePath string, fileType string) ([]string, error) {

	inputFilePath, _ = filepath.Abs(inputFilePath)

	//判断是否存在
	fileInfo, err := os.Stat(inputFilePath)
	if err != nil {
		return nil, err
	}

	//判断是否为文件夹，不是文件夹，是文件直接返回
	if !fileInfo.IsDir() {
		return []string{inputFilePath}, nil
	}

	var result []string
	fileNameSlice, _ := ioutil.ReadDir(inputFilePath)
	for _, file := range fileNameSlice {
		if filepath.Ext(file.Name()) == fileType {
			result = append(result, filepath.Join(inputFilePath, file.Name()))
		}
	}

	return result, nil
}
