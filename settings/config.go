package settings

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
)

type Config struct {
	NodeName string

	// Memberlist configs
	ClusterMemberAddress string
	ClusterBindAddress   string

	SeedBindAddress   string
	StreamBindAddress string

	HTTPBindPort string

	NodeRetransmitMux int
	NodeSuspicionMult int
	NodeProbeInterval int // in milliseconds
	NodeProbeTimeout int // in milliseconds
	NodeGossipInterval int // in milliseconds


	// Seeding/Streaming config
	SeedFilename string
	NumSegments int

	SeedFrequency int // How frequenty to seed in milliseconds
	ExchangeFrequency int // How frequently the node should try to exchange

	ClusterRetransmitMux int
	ClusterSuspicionMult int
	ClusterProbeInterval int // in milliseconds
	ClusterProbeTimeout int // in milliseconds
	ClusterGossipInterval int // in milliseconds

	FileExpiry int // time in seconds after which a file becomes stale

}

func LoadConfig(fname string) Config {
	file, err := ioutil.ReadFile(fname)

	if err != nil {
		log.Fatal(err)
		os.Exit(-1)
	}

	var config Config
	err = json.Unmarshal(file, &config)

	if err != nil {
		log.Fatal(err)
		os.Exit(-1)
	}

	return config
}
