package test

import (
	"fmt"
	"io/fs"
	"os"
	"testing"
)

func TestCreateFile(t *testing.T) {
	filePath := "/Users/lixiaoqing/Desktop/tmp/pg1/1.txt"
	_ = os.MkdirAll(filePath, fs.ModePerm)
	_ = os.Remove(filePath)
	newFile, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	fmt.Println(err)
	fmt.Println(newFile.Name())

	filePath = "/Users/lixiaoqing/Desktop/tmp/pg1/2.txt"
	_ = os.MkdirAll(filePath, fs.ModePerm)
	_ = os.Remove(filePath)
	newFile, err = os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	fmt.Println(err)
	fmt.Println(newFile.Name())

}
