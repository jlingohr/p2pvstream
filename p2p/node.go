package p2p

import (
	"fmt"
	"github.com/hashicorp/memberlist"
	"github.com/jlingohr/p2pvstream/fileutil"
	"github.com/jlingohr/p2pvstream/hls"
	"github.com/jlingohr/p2pvstream/message"
	"github.com/jlingohr/p2pvstream/settings"
	"github.com/jlingohr/p2pvstream/streaming"
	"github.com/jlingohr/p2pvstream/stringutil"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"time"
)

type Node struct {
	broadcasts *memberlist.TransmitLimitedQueue
	mlist      *memberlist.Memberlist
	config     settings.Config
	comm       streaming.ClientStreamComm
}

func NewNode(config settings.Config) (*Node, error) {
	addrParts := strings.Split(config.ClusterBindAddress, ":")

	if len(addrParts) != 2 {
		log.Fatal("Invalid address supplied")
	}
	bindAddr := addrParts[0]
	bindPort := addrParts[1]

	c := memberlist.DefaultWANConfig()
	c.BindPort, _ = strconv.Atoi(bindPort)
	c.BindAddr = bindAddr
	c.PushPullInterval = 0 // disable push/pull

	c.Name = config.NodeName

	if config.NodeGossipInterval != 0 {
		c.GossipInterval = time.Duration(config.NodeGossipInterval) * time.Millisecond
	}

	if config.NodeProbeInterval != 0 {
		c.ProbeInterval = time.Duration(config.NodeProbeInterval) * time.Millisecond
	}

	if config.NodeProbeTimeout != 0 {
		c.ProbeTimeout = time.Duration(config.NodeProbeTimeout) * time.Millisecond
	}

	if config.NodeSuspicionMult != 0 {
		c.SuspicionMult = config.NodeSuspicionMult
	}

	m, err := memberlist.Create(c)

	if err != nil {
		return &Node{}, err
	}

	b := &memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			return m.NumMembers()
		},
		RetransmitMult: config.NodeRetransmitMux,
	}

	comm := streaming.ClientStreamComm{
		DiscoveredFiles:   make(chan streaming.DiscoveredFile),
		StreamFileRequest: make(chan streaming.DiscoveredFile),
		StartStream:       make(chan streaming.ClientStartStreamReq),
	}

	n := &Node{
		config:     config,
		broadcasts: b,
		mlist:      m,
		comm:       comm,
	}

	c.Delegate = n

	if len(config.ClusterMemberAddress) > 0 {
		m.Join([]string{config.ClusterMemberAddress})
	}

	return n, nil
}

func (n *Node) Start() {
	if len(n.config.SeedFilename) > 0 {
		// start seeding routines
		// todo start another cluster here for seeding on n.config.StreamBindAddress
		log.Printf("Started seeding file: %s\n", n.config.SeedFilename)
		seeder, err := NewSeeder(n.config)

		if err != nil {
			log.Fatal(err)
		}

		seeder.Start()

		go n.AnnounceSeed()
	}

	// start streaming routines
	go n.handleClients()
	go n.handleClientComm()

}

func (n *Node) handleMsg(msg message.P2PMessage) {

	switch msg.MsgType {
	case message.SeedAnnounceMsg:
		announceMsg, err := message.DecodeSeedAnnounce(msg.Payload)

		if err != nil {
			log.Println(err)
			return
		}

		discoveredFile := streaming.DiscoveredFile{
			Filename:  announceMsg.Filename,
			Timestamp: announceMsg.Timestamp,
			NodeName:  announceMsg.NodeName,
		}

		n.comm.DiscoveredFiles <- discoveredFile

	case message.MetadataRequestMsg:
		req, err := message.DecodeMetadataRequest(msg.Payload)

		if err != nil {
			log.Println(err)
			return
		}

		n.sendMetadataResponse(req)
		log.Println("[MetadataRequestMsg] || From:", req.OriginNodeName, "|| FileName:", req.Filename)

	case message.MetadataResponseMsg:
		resp, err := message.DecodeMetadataResponse(msg.Payload)

		if err != nil {
			log.Println(err)
			return
		}

		n.handleMetadataResponse(resp)
		log.Println("[MetadataResponseMsg] || From:", resp.NodeName, "|| FileName:", resp.MetaFilename)

	default:
		log.Println("Unhandled message type in Node:", msg.MsgType)
	}
}

func (n *Node) AnnounceSeed() {
	announceMsg := message.SeedFileAnnounce{
		Filename: n.config.SeedFilename,
		NodeName: n.config.NodeName,
	}

	for {
		announceMsg.Timestamp = time.Now().Unix()
		encodedMsg, err := message.EncodeMessage(announceMsg)

		if err != nil {
			log.Fatal("Failed to encode announce seed msg.")
			return
		}

		p2pMsg := message.P2PMessage{
			MsgType: message.SeedAnnounceMsg,
			Payload: encodedMsg,
		}
		// todo mutex broadcasts???
		n.broadcasts.QueueBroadcast(message.BroadcastMsg{Msg: p2pMsg})
		time.Sleep((time.Duration(n.config.FileExpiry - 5)) * time.Second)
	}
}

func (n *Node) handleClientComm() {
	for file := range n.comm.StreamFileRequest {
		// Request from the client to stream filename
		log.Println("Requesting metadata file for", file.Filename)

		// todo should probably mux this
		for i := range n.mlist.Members() {
			node := n.mlist.Members()[i]
			if node.Name == file.NodeName {
				req := message.MetadataRequest{
					Filename:       file.Filename,
					OriginNodeName: n.config.NodeName,
				}

				msg, err := message.EncodeMessage(req)
				p2pMsg, err := message.EncodeMessage(message.P2PMessage{MsgType: message.MetadataRequestMsg, Payload: msg})

				if err != nil {
					log.Println(err)
					break
				}

				log.Println("Sent metadata request to", node.Name)
				err = n.mlist.SendReliable(node, p2pMsg)

				if err != nil {
					log.Println(err)
				}
			}
		}
	}
}

func (n *Node) sendMetadataResponse(req message.MetadataRequest) {
	if req.Filename != n.config.SeedFilename {
		log.Println("Invalid MetadataRequest, the node is not seeding requested file:", req.Filename)
		return
	}

	// todo should probably mux this
	for i := range n.mlist.Members() {
		node := n.mlist.Members()[i]
		if node.Name == req.OriginNodeName {
			noExtFilename := stringutil.RemoveFilenameExt(req.Filename)
			dirPath := hls.SEED_HLS_PATH + noExtFilename
			metadataFilepath := fmt.Sprintf("%s/%s.m3u8", dirPath, noExtFilename)

			data, err := ioutil.ReadFile(metadataFilepath)

			if err != nil {
				log.Println(err)
				return
			}

			numFiles, err := fileutil.FileCount(dirPath)

			if err != nil {
				log.Println(err)
				return
			}

			resp := message.MetadataResponse{
				NodeName:     n.config.NodeName,
				NodeAddr:     n.config.SeedBindAddress,
				File:         data,
				MetaFilename: noExtFilename + ".m3u8",
				NumOfParts:   numFiles - 1,
			}

			msg, err := message.EncodeMessage(resp)

			if err != nil {
				log.Println(err)
				return
			}

			p2pMsg, err := message.EncodeMessage(message.P2PMessage{MsgType: message.MetadataResponseMsg, Payload: msg})

			if err != nil {
				log.Println(err)
				return
			}

			n.mlist.SendReliable(node, p2pMsg)
			return
		}
	}

	log.Printf("Could not find node %s, no MetadataResponse sent.", req.OriginNodeName)
}

func (n *Node) handleMetadataResponse(resp message.MetadataResponse) {
	noExtFilename := stringutil.RemoveFilenameExt(resp.MetaFilename)
	dirPath := fmt.Sprintf("%s%s/%s/", hls.CACHE_PATH, n.config.NodeName, noExtFilename)

	cachedMetaFilepath := fmt.Sprintf("%s/%s", dirPath, resp.MetaFilename)

	// Create directory if does not exist
	err := fileutil.CreateDirIfNotExists(dirPath)

	if err != nil {
		log.Println(err)
		return
	}

	err = ioutil.WriteFile(cachedMetaFilepath, resp.File, 0644)

	if err != nil {
		log.Println(err)
		return
	}

	log.Println("Successfully cached m3u8 file:", resp.MetaFilename)

	// got metadata, can join the stream cluster now

	n.startStreamer(resp)
}

func (n *Node) startStreamer(resp message.MetadataResponse) {
	file := streaming.StreamedFile{
		MetaName:      resp.MetaFilename,
		NoExtFilename: stringutil.RemoveFilenameExt(resp.MetaFilename),
		SegCount:      resp.NumOfParts,
	}

	stopStream := make(chan struct{})

	config := streamerConfig{
		global: n.config,
		streamedFile:file,
		seederAddr:resp.NodeAddr,
		seederName:resp.NodeName,
	}

	streamer, err := NewStreamer(config)

	if err != nil {
		log.Println(err)
		return
	}

	startStreamReq := streaming.ClientStartStreamReq{
		File:file,
		StopStream:stopStream,
	}

	n.comm.StartStream <- startStreamReq
	streamer.Start(stopStream)
}

func (n *Node) handleClients() {
	sh := streaming.NewClientStreamHandler(n.config, n.comm)
	sh.Start()
}

// Delegate protocol implementation for Node

func (n *Node) NodeMeta(limit int) []byte {
	return []byte{}
}

func (n *Node) NotifyMsg(b []byte) {
	// all messages sent/broadcast must be wrapped into P2PMessage
	msg, err := message.DecodeP2PMessage(b)

	if err != nil {
		log.Println(err)
		return
	}

	go n.handleMsg(msg)
}

func (n *Node) GetBroadcasts(overhead, limit int) [][]byte {
	return n.broadcasts.GetBroadcasts(overhead, limit)
}

func (n *Node) LocalState(join bool) []byte {
	//stub, we don't sync state
	return []byte{}
}

func (n *Node) MergeRemoteState(buf []byte, join bool) {
	//stub, we don't sync state
}
