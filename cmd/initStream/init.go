package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	if len(os.Args) != 3 {
		log.Fatalf("Expect: go run main.go [PublicIP] [streamName]")
	}

	addr := os.Args[1]
	stream := os.Args[2]
	url := fmt.Sprintf("http://%s/initStream/%s.mp4/", addr, stream)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Fatal(err)
	}

	client := http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}
	resp.Body.Close()

	time.Sleep(1  * time.Second)

	resp, err = client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}

	log.Println(string(content))
}
