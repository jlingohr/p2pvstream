package streaming

import (
	"fmt"
	"github.com/gorilla/mux"
	"github.com/jlingohr/p2pvstream/hls"
	"github.com/jlingohr/p2pvstream/settings"
	"github.com/jlingohr/p2pvstream/stringutil"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

type DiscoveredFile struct {
	Filename  string
	NodeName  string
	Timestamp int64
}

type M3UReq struct {
	MetaName     string
	ResponseChan chan string
}

type SegmentReq struct {
	SegName      string
	MetaName     string
	ResponseChan chan string
}

type StreamedFile struct {
	NoExtFilename string
	MetaName      string
	SegCount      int
}

type ClientStartStreamReq struct {
	File StreamedFile
	StopStream chan struct{}
}

type ClientStreamHandler struct {
	IsStreaming     bool
	File            StreamedFile
	SegReqs         chan SegmentReq
	M3UReqs         chan M3UReq
	StopStreaming   chan struct{}
	DiscoveredFiles map[string]DiscoveredFile
	Comm            ClientStreamComm
	config          settings.Config
	m               sync.RWMutex
}

type ClientStreamComm struct {
	DiscoveredFiles   chan DiscoveredFile
	StreamFileRequest chan DiscoveredFile
	StartStream       chan ClientStartStreamReq
}

func NewClientStreamHandler(config settings.Config, comm ClientStreamComm) *ClientStreamHandler {
	sh := &ClientStreamHandler{
		IsStreaming:     false,
		M3UReqs:         make(chan M3UReq),
		SegReqs:         make(chan SegmentReq),
		StopStreaming: make(chan struct{}),
		File:            StreamedFile{NoExtFilename: "hlstest", MetaName: "hlstest.m3u8", SegCount: 10},
		DiscoveredFiles: make(map[string]DiscoveredFile),
		Comm:            comm,
		config:          config,
	}

	return sh
}

func (sh *ClientStreamHandler) Start() {
	go sh.handleComm()
	go sh.startHttpServer()
}

func (sh *ClientStreamHandler) startHttpServer() {
	router := mux.NewRouter()
	router.StrictSlash(true)

	router.HandleFunc("/stream/{fname}/{segName}/", sh.ServeTs).Methods("GET")
	router.HandleFunc("/stream/{fname}/", sh.ServeMeta).Methods("GET")
	router.HandleFunc("/initStream/{fname}/", sh.HandleStreamInit).Methods("GET")
	router.HandleFunc("/stopStream/", sh.HandleStopStream).Methods("GET")
	router.HandleFunc("/", sh.ServeDiscoveredFiles).Methods("GET")

	log.Printf("Starting HLS streaming server on port %s", sh.config.HTTPBindPort)
	log.Printf("Stream will be accessible via http://%s/stream/[filename.m3u8]/", sh.config.HTTPBindPort)
	log.Fatal(http.ListenAndServe(sh.config.HTTPBindPort, router))
}

func (sh *ClientStreamHandler) handleComm() {
	// todo extract constants somewhere
	removeStaleTicker := time.NewTicker((time.Duration(sh.config.FileExpiry) + 5) * time.Second).C

	for {
		select {
		case file := <-sh.Comm.DiscoveredFiles:
			sh.m.Lock()
			sh.DiscoveredFiles[file.Filename] = file
			sh.m.Unlock()
		case <-removeStaleTicker:
			filesToRemove := make([]string, 0)

			sh.m.RLock()
			for _, file := range sh.DiscoveredFiles {
				if time.Now().After(time.Unix(file.Timestamp, 0).Add(time.Duration(sh.config.FileExpiry) * time.Second)) {
					filesToRemove = append(filesToRemove, file.Filename)
				}
			}
			sh.m.RUnlock()

			sh.m.Lock()
			for _, fname := range filesToRemove {
				delete(sh.DiscoveredFiles, fname)
			}
			sh.m.Unlock()

			log.Println("Discovered files:", sh.DiscoveredFiles)
		case req := <-sh.Comm.StartStream:
			if sh.IsStreaming {
				// should never reach here
				log.Println("Attempted to start a stream without properly terminating previous session.")
				continue
			}

			sh.File = req.File

			go sh.handleHLSRequests(req.StopStream)
		}
	}
}

func (sh *ClientStreamHandler) handleHLSRequests(stop chan struct{}) {
	sh.m.Lock()
	sh.IsStreaming = true
	sh.StopStreaming = stop
	sh.m.Unlock()

	pendingReqs := make(map[string]chan string)
	ticker := time.NewTicker(3 * time.Second).C

	for {
		select {
		case <- sh.StopStreaming:
			sh.m.Lock()
			sh.IsStreaming = false
			sh.m.Unlock()

			return
		case req := <-sh.SegReqs:
			noExtFilename := stringutil.RemoveFilenameExt(req.MetaName)

			if noExtFilename != sh.File.NoExtFilename {
				req.ResponseChan <- "" // return no content
				continue
			}

			cachedPath := fmt.Sprintf("%s%s/%s/%s", hls.CACHE_PATH, sh.config.NodeName, noExtFilename, req.SegName)

			if _, err := os.Stat(cachedPath); os.IsNotExist(err) {
				pendingReqs[cachedPath] = req.ResponseChan
				continue
			}

			req.ResponseChan <- cachedPath
		case req := <-sh.M3UReqs:
			noExtFilename := stringutil.RemoveFilenameExt(req.MetaName)

			if noExtFilename != sh.File.NoExtFilename {
				req.ResponseChan <- "" // return no content
				continue
			}

			cachedPath := fmt.Sprintf("%s%s/%s/%s", hls.CACHE_PATH, sh.config.NodeName, noExtFilename, req.MetaName)

			if _, err := os.Stat(cachedPath); os.IsNotExist(err) {
				req.ResponseChan <- ""
				continue
			}

			req.ResponseChan <- cachedPath
		case <- ticker:
			if len(pendingReqs) == 0 {
				continue
			}
			responded := make([]string, 0)

			for path := range pendingReqs {
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					responded = append(responded, path)
					respChan := pendingReqs[path]
					respChan <- path
				}
			}

			for _, path := range responded {
				delete(pendingReqs, path)
			}
		}
	}
}
