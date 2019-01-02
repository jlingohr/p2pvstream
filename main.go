package main

import (
	"flag"
	"github.com/jlingohr/p2pvstream/hls"
	"github.com/jlingohr/p2pvstream/p2p"
	"github.com/jlingohr/p2pvstream/settings"
	"log"
	"os"
	"sync"
)

func main() {
	configFname := flag.String("config", "config.json", "node json settings file")
	flag.Parse()
	config := settings.LoadConfig(*configFname)

	if err := hls.WipeCache(config.NodeName); err != nil {
		log.Println("Wipe cached: ", err)
	}

	if len(config.SeedFilename) > 0 {
		hls.GenerateHLS(config.SeedFilename)
	}

	p2pNode, err := p2p.NewNode(config)

	if err != nil {
		log.Fatal(err)
		os.Exit(-1)
	}

	var wg sync.WaitGroup
	wg.Add(1)

	p2pNode.Start()

	wg.Wait()
}
