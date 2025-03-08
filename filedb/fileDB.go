package filedb

import (
	"fmt"
	"killerDB/utils"
	"os"
)

func SaveData(path string, data []byte) error {
	fp, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0664)
	if err != nil {
		return err
	}
	defer fp.Close()
	_, err = fp.Write(data)
	return err
}

func SaveDataWithBetterPersistence(path string, data []byte) error {
	tmpFilePath := fmt.Sprintf("%s.tmp.%d", path, utils.RandomInt())

	fp, err := os.OpenFile(tmpFilePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0664)
	if err != nil {
		return err
	}
	defer fp.Close()

	_, err = fp.Write(data)
	if err != nil {
		os.Remove(tmpFilePath)
		return err
	}
	return os.Rename(tmpFilePath, path)
}

func SaveDataWithBetterPersistenceUsingFsync(path string, data []byte) error {
	tmpFilePath := fmt.Sprintf("%s.tmp.%d", path, utils.RandomInt())

	fp, err := os.OpenFile(tmpFilePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0664)
	if err != nil {
		return err
	}
	defer fp.Close()

	_, err = fp.Write(data)
	if err != nil {
		os.Remove(tmpFilePath)
		return err
	}

	err = fp.Sync()
	if err != nil {
		os.Remove(tmpFilePath)
		return err
	}

	return os.Rename(tmpFilePath, path)
}

func LogAppend(fp *os.File, data []byte) error {
	_, err := fp.Write(data)
	if err != nil {
		return err
	}
	return fp.Sync()
}
