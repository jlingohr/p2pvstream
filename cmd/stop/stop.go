package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatalf("Expect: go run main.go [publicIP]")
	}
	addr := os.Args[1]

	url := fmt.Sprintf("http://%s/stopStream/", addr)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Fatal(err)
	}
	client := http.Client{}

	_, err = client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Stopped streaming")
}
