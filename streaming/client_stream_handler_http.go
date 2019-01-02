package streaming

import (
	"fmt"
	"github.com/gorilla/mux"
	"html/template"
	"net/http"
)

func (sh *ClientStreamHandler) ServeTs(rw http.ResponseWriter, r *http.Request) {
	if !sh.IsStreaming {
		rw.WriteHeader(http.StatusNoContent)
	}

	reqVars := mux.Vars(r)
	segName := reqVars["segName"]
	metaName := reqVars["fname"]

	responseChan := make(chan string)

	sh.SegReqs <- SegmentReq{
		ResponseChan: responseChan,
		MetaName:     metaName,
		SegName:      segName,
	}

	cachedFname := <-responseChan

	if len(cachedFname) == 0 {
		rw.WriteHeader(http.StatusNoContent)
	}

	http.ServeFile(rw, r, cachedFname)
	rw.Header().Set("Content-Type", "video/MP2T")
}

func (sh *ClientStreamHandler) ServeMeta(rw http.ResponseWriter, r *http.Request) {
	if !sh.IsStreaming {
		rw.WriteHeader(http.StatusNoContent)
	}

	metaName := mux.Vars(r)["fname"]
	responseChan := make(chan string)

	sh.M3UReqs <- M3UReq{
		ResponseChan: responseChan,
		MetaName:     metaName,
	}

	cachedFname := <-responseChan

	if len(cachedFname) == 0 {
		rw.WriteHeader(http.StatusNoContent)
	}

	http.ServeFile(rw, r, cachedFname)
	rw.Header().Set("Content-Type", "application/x-mpegURL")
}

type DiscoveredFilesPage struct {
	Filenames []string
}

func (sh *ClientStreamHandler) ServeDiscoveredFiles(rw http.ResponseWriter, r *http.Request) {
	pageData := DiscoveredFilesPage{
		Filenames: make([]string, 0),
	}

	sh.m.RLock()
	for fname := range sh.DiscoveredFiles {
		pageData.Filenames = append(pageData.Filenames, fname)
	}
	sh.m.RUnlock()

	if len(pageData.Filenames) == 0 {
		http.ServeFile(rw, r, "static/index.html")
	} else {
		t, _ := template.ParseFiles("static/discovered_files.html")
		t.Execute(rw, pageData)
	}
}

type StreamURLPage struct {
	Filename  string
	StreamURL string
}

type ErrorPage struct {
	Error string
}

func (sh *ClientStreamHandler) HandleStreamInit(rw http.ResponseWriter, r *http.Request) {
	reqVars := mux.Vars(r)
	fname := reqVars["fname"]

	sh.m.RLock()
	streaming := sh.IsStreaming
	sh.m.RUnlock()

	if streaming {
		// redirect to stream_url page
		noExtFname := sh.File.NoExtFilename
		url := fmt.Sprintf("http://%s/stream/%s/", sh.config.HTTPBindPort, sh.File.MetaName)
		pageData := StreamURLPage{
			Filename:  noExtFname,
			StreamURL: url,
		}

		t, _ := template.ParseFiles("static/stream_url.html")
		t.Execute(rw, pageData)
		return
	}

	sh.m.RLock()
	requestedFile, ok := sh.DiscoveredFiles[fname]
	sh.m.RUnlock()

	if !ok {
		// return an error
		pageData := ErrorPage{
			Error: "The file you are requesting has expired or does not exist in the network.",
		}

		t, _ := template.ParseFiles("static/error.html")
		t.Execute(rw, pageData)
		return
	}

	sh.Comm.StreamFileRequest <- requestedFile

	http.Redirect(rw, r, fmt.Sprintf("/initStream/%s/", fname), http.StatusOK)
}

func (sh *ClientStreamHandler) HandleStopStream(rw http.ResponseWriter, r *http.Request) {
	sh.m.RLock()
	streaming := sh.IsStreaming
	sh.m.RUnlock()

	if streaming {
		close(sh.StopStreaming)
	}

	http.Redirect(rw, r, "/", http.StatusOK)
}
