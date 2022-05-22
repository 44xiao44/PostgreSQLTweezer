package test

//FilterFileByRule1 分割文件
//func FilterFileByRule1(filePath string, newFilePath string, min float64, max float64) {
//
//	csvFile, err := os.OpenFile(filePath, os.O_RDONLY, 0666)
//	if err != nil {
//		log.Fatalf("can not open the file, err is %+v", err)
//	}
//	defer func() {
//		_ = csvFile.Close()
//	}()
//	reader := bufio.NewReader(csvFile)
//
//	//新文件
//	_ = os.Remove(newFilePath)
//	newFile, err := os.OpenFile(newFilePath, os.O_RDWR|os.O_CREATE, 0666)
//	defer func() {
//		_ = newFile.Close()
//	}()
//
//	writer := bufio.NewWriter(newFile)
//
//	i := 0
//	for {
//		i++
//		if i%10000 == 0 {
//			fmt.Println(i)
//			_ = writer.Flush()
//		}
//		//如果遇到空行，跳过
//		line, err := reader.ReadString('\n')
//
//		durationTime := stringOperate.GetRecordInfo(line).DurationTime
//		if durationTime >= min && max >= durationTime {
//			_, _ = writer.WriteString(line)
//		}
//
//		//文件非正常结束
//		if err != nil && err != io.EOF {
//			log.Fatalf("File format error, record on line:  %+v", err)
//		}
//
//		// 文件结束判断
//		if err != nil && err == io.EOF {
//			break
//		}
//	}
//	_ = writer.Flush()
//}

////SplitFileByRule 根据规则分割成多个文件
//func SplitFileByRule(filePath string, splitInfoSlice []*SplitInfo) {
//
//	type fileInfo struct {
//		File   *os.File
//		Writer *bufio.Writer
//	}
//
//	fileInfoMap := make(map[string]fileInfo)
//
//	// 循环创建新文件
//	for _, splitInfo := range splitInfoSlice {
//		// 文件名
//		fileName := splitInfo.OutputFileFillName
//		fmt.Println(fileName)
//
//		//删除旧文件
//		_ = os.Remove(fileName)
//		//创建新文件
//		newFile, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0666)
//		if err != nil {
//			log.Fatalf("creat file error")
//		}
//
//		writer := bufio.NewWriter(newFile)
//		fileInfoMap[fileName] = fileInfo{newFile, writer}
//	}
//
//	csvFile, err := os.OpenFile(filePath, os.O_RDONLY, 0666)
//	if err != nil {
//		log.Fatalf("can not open the file, err is %+v", err)
//	}
//	defer func() {
//		_ = csvFile.Close()
//	}()
//	reader := bufio.NewReader(csvFile)
//
//	i := 0
//	for {
//		i++
//		if i%2000 == 0 {
//			fmt.Println(i)
//			// 循环flash
//			for _, fileInfo := range fileInfoMap {
//				_ = fileInfo.Writer.Flush()
//			}
//		}
//
//		//获取行数据
//		line, err := reader.ReadString('\n')
//		//如果行数据有值才处理
//		if len(line) > 0 {
//			recordInfo := stringOperate.GetRecordInfo(&line)
//			for _, splitInfo := range splitInfoSlice {
//				//如果满足条件写入对应的文件
//				if recordInfo.DurationTime >= splitInfo.MinDuration && splitInfo.MaxDuration >= recordInfo.DurationTime &&
//					(recordInfo.UserName == splitInfo.UserName || splitInfo.UserName == "*") &&
//					(recordInfo.DataBaseName == splitInfo.DataBaseName || splitInfo.DataBaseName == "*") {
//					writer := fileInfoMap[splitInfo.OutputFileFillName].Writer
//					_, _ = writer.WriteString(line)
//				}
//			}
//		}
//		//文件非正常结束
//		if err != nil && err != io.EOF {
//			log.Fatalf("File format error, record on line:  %+v", err)
//		}
//
//		// 文件结束判断
//		if err != nil && err == io.EOF {
//			break
//		}
//	}
//
//	// 最后的流flush，关闭文件
//	for _, fileInfo := range fileInfoMap {
//		// 写入最后的流
//		_ = fileInfo.Writer.Flush()
//		//关闭文件
//		_ = fileInfo.File.Close()
//	}
//}
