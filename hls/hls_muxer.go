package hls

import (
	"github.com/jlingohr/p2pvstream/stringutil"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
)

const CACHE_PATH = "./cached/"
const SEED_PATH = "./seed/"
const SEED_HLS_PATH = SEED_PATH + "hls/"

func GenerateHLS(sourceFname string) {
	files, err := ioutil.ReadDir(SEED_PATH)

	if err != nil {
		log.Fatal(err)
	}

	if len(files) > 0 {
		log.Printf("Muxing the contents of %s to HLS, output dir: %s", SEED_PATH, SEED_HLS_PATH)
	}

	for _, f := range files {
		if f.IsDir() {
			continue
		}

		fname := f.Name()

		if fname != sourceFname {
			continue
		}

		noExtFilename := stringutil.RemoveFilenameExt(fname)

		hlsDirPath := SEED_HLS_PATH + noExtFilename

		// check if HLS already exists
		if _, err := os.Stat(hlsDirPath); !os.IsNotExist(err) {
			log.Printf("File %s has already been muxed to HLS, skipping.", fname)
			return
		} else {
			err := os.MkdirAll(hlsDirPath, os.ModePerm)
			if err != nil {
				log.Fatal(err)
			}
		}

		m3u8Filepath := hlsDirPath + "/" + noExtFilename + ".m3u8"

		err := exec.Command(
			"ffmpeg",
			"-i", SEED_PATH+fname,
			"-codec:", "copy",
			"-start_number", "0",
			"-hls_time", "10",
			"-hls_list_size", "0",
			"-f", "hls", m3u8Filepath).Run()

		if err != nil {
			log.Fatal(err)
		}

		return
	}

	log.Fatal("Could not find source file:", sourceFname)
}

func WipeCache(nodeName string) error {
	return os.RemoveAll(CACHE_PATH + nodeName)
}
