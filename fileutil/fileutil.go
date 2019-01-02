package fileutil

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
)

func FileCount(path string) (int, error) {
	i := 0
	files, err := ioutil.ReadDir(path)

	if err != nil {
		return 0, err
	}

	for _, file := range files {
		if !file.IsDir() {
			i++
		}
	}

	return i, nil
}

func CreateDirIfNotExists(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		err := os.MkdirAll(path, os.ModePerm)

		if err != nil {
			log.Println(err)
			return err
		}
	}

	return nil
}

func RemoveContents(path string) error {
	d, err := os.Open(path)

	if err != nil {
		return err
	}

	defer d.Close()

	names, err := d.Readdirnames(-1)

	if err != nil {
		return err
	}

	for _, name := range names {
		err = os.RemoveAll(filepath.Join(path, name))
		if err != nil {
			return err
		}
	}

	return nil
}
