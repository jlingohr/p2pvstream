package main

import (
	"bufio"
	"errors"
	"fmt"
	"golang.org/x/net/html"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	Exit = "exit"
	Init = "init"
	Stop = "stop"
	List = "ls"
)

//Manage streams

var wg = sync.WaitGroup{}

type streamManager struct {
	requests chan request
	client http.Client
	streams map[string]string
}

func newStreamManager(ips []string) *streamManager {
	streams := make(map[string]string)
	for idx, addr := range ips {
		t := strconv.Itoa(idx+1)
		streams[t] = addr
	}

	sm := &streamManager{
		requests: make(chan request),
		client: http.Client{},
		streams: streams,
	}
	return sm
}

func (sm *streamManager) run() {

	defer wg.Done()

	for req := range sm.requests {
		switch req.cmd.command {
		case Init:
			links, err := sm.handleInit(req.cmd)
			result := result{value: links, err: err }
			req.response <- result
		case Stop:
			status, err := sm.handleStop(req.cmd)
			result := result{value: status, err: err}
			req.response <- result
		case List:
			links, err := sm.handleList(req.cmd)
			result := result{value: links, err: err }
			req.response <- result
		default:
			result := result{err: errors.New("Invalid command")}
			req.response <- result
		}
	}
}

func (sm *streamManager) handleInit(cmd Command) ([]string, error) {
	idx := cmd.args[0]
	addr, ok := sm.streams[idx]
	if !ok {
		return nil, errors.New("Invalid stream given")
	}
	stream := cmd.args[1]
	url := fmt.Sprintf("http://%s/initStream/%s.mp4/", addr, stream)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := sm.client.Do(req)
	if err != nil {
		return nil, err
	}
	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	resp.Body.Close()

	time.Sleep(1  * time.Second)

	resp, _ = sm.client.Do(req)
	defer resp.Body.Close()

	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return []string{string(content)}, nil
}

func (sm *streamManager) handleStop(cmd Command) ([]string, error) {
	idx := cmd.args[0]
	addr, ok := sm.streams[idx]
	if !ok {
		return nil, errors.New("Invalid stream given")
	}

	url := fmt.Sprintf("http://%s/stopStream/", addr)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	_, err = sm.client.Do(req)
	if err != nil {
		return nil, err
	}
	return []string{fmt.Sprintf("Stopped stream on %s", addr)}, nil
}

func (sm *streamManager) handleList(cmd Command) ([]string, error) {
	idx := cmd.args[0]
	addr, ok := sm.streams[idx]
	if !ok {
		return nil, errors.New("Invalid stream given")
	}
	url := fmt.Sprintf("http://%s/", addr)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := sm.client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	links := getLinks(resp.Body)
	return links, nil
}

func getLinks(body io.Reader) []string {
	var links []string
	z := html.NewTokenizer(body)
	for {
		tt := z.Next()

		switch tt {
		case html.ErrorToken:
			//todo: links list shoudn't contain duplicates
			return links
		case html.StartTagToken, html.EndTagToken:
			token := z.Token()
			if "a" == token.Data {
				for _, attr := range token.Attr {
					if attr.Key == "href" {
						links = append(links, attr.Val)
					}

				}
			}

		}
	}
}

func (sm *streamManager) stop() {
	close(sm.requests)
}

type result struct {
	value interface{}
	err error
}

type request struct {
	cmd Command
	response chan<- result
}

func (sm *streamManager) execute(cmd Command) (interface{}, error) {
	response := make(chan result)
	req := request{cmd: cmd, response: response}
	sm.requests <- req
	res := <- response
	return res.value, res.err
}

//Handle terminal input

type Terminal struct {

}

type Command struct {
	command string
	args []string
}

func (t *Terminal) run(sm *streamManager) {

	defer wg.Done()

	removeLeading := regexp.MustCompile(`^[\s\p{Zs}]+|[\s\p{Zs}]+$`)
	removeInside := regexp.MustCompile(`[\s\p{Zs}]{2,}`)

	clean := func(s string) string {
		ss := removeLeading.ReplaceAllString(s, "")
		return removeInside.ReplaceAllString(ss, " ")
	}

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print(">> ")
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
		line := clean(input)
		cmd := t.parseCommand(line)

		if cmd.command == Exit {
			sm.stop()
			return
		} else {
			res, err := sm.execute(cmd)
			if err != nil {
				fmt.Println(err)
			} else {
				fmt.Println(res)
			}
		}

	}
}

func (t *Terminal) parseCommand(line string) Command {
	s := strings.Split(line, " ")
	cmd := s[0]
	args := make([]string, 0)
	for _, arg := range s[1:] {
		args = append(args, arg)
	}

	c := Command{
		command: cmd,
		args: args,
	}

	return c
}

//helper functions

func getPublicAddresses(fname string) ([]string, error) {
	data, err := ioutil.ReadFile(fname)
	if err != nil {
		log.Fatal(err)
	}
	ips := strings.Split(string(data), "\n")
	return ips, nil
}

func main() {
	if len(os.Args) != 2 {
		log.Fatalf("Expect: go run manage.go [ipFile]")
	}

	ips, err := getPublicAddresses(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}

	log.Println(ips)

	sm := newStreamManager(ips)
	wg.Add(1)
	go sm.run()

	t := &Terminal{}
	wg.Add(1)
	go t.run(sm)

	wg.Wait()
}
