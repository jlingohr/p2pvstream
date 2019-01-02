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
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Seeder struct {
	mlist         *memberlist.Memberlist
	broadcasts    *memberlist.TransmitLimitedQueue
	nodeToSegNum  map[string]int             // Node name to segment number map
	segNumToNodes map[int][]*memberlist.Node // Segment number to nodes map
	filePartition map[int][]int
	m             sync.RWMutex
	config        settings.Config
	file          streaming.StreamedFile
}

func NewSeeder(config settings.Config) (*Seeder, error) {
	addrParts := strings.Split(config.SeedBindAddress, ":")

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

	if config.ClusterGossipInterval != 0 {
		c.GossipInterval = time.Duration(config.ClusterGossipInterval) * time.Millisecond
	}

	if config.ClusterProbeInterval != 0 {
		c.ProbeInterval = time.Duration(config.ClusterProbeInterval) * time.Millisecond
	}

	if config.ClusterProbeTimeout != 0 {
		c.ProbeTimeout = time.Duration(config.ClusterProbeTimeout) * time.Millisecond
	}

	if config.ClusterSuspicionMult != 0 {
		c.SuspicionMult = config.ClusterSuspicionMult
	}

	m, err := memberlist.Create(c)

	if err != nil {
		return &Seeder{}, err
	}

	b := &memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			return m.NumMembers()
		},
		RetransmitMult: 3,
	}

	nodeToSegNum := map[string]int{config.NodeName: -1}
	segNumToNodes := map[int][]*memberlist.Node{-1: m.Members()}

	s := &Seeder{
		broadcasts:    b,
		mlist:         m,
		nodeToSegNum:  nodeToSegNum,
		segNumToNodes: segNumToNodes,
		m:             sync.RWMutex{},
		config:        config,
	}

	s.file, err = s.getFile()
	if err != nil {
		return &Seeder{}, err
	}
	s.filePartition = s.getFilePartition()

	c.Delegate = s
	c.Events = s

	log.Println("File Partition:", s.filePartition)

	return s, nil
}

func (s *Seeder) getFile() (streaming.StreamedFile, error) {

	filename := stringutil.RemoveFilenameExt(s.config.SeedFilename)
	dirPath := hls.SEED_HLS_PATH + filename
	numFiles, err := fileutil.FileCount(dirPath)

	if err != nil {
		log.Println(err)
		return streaming.StreamedFile{}, err
	}

	return streaming.StreamedFile{
		NoExtFilename: filename,
		SegCount:      numFiles - 1,
	}, nil
}

func (s *Seeder) getFilePartition() map[int][]int {

	clusterPartition := map[int][]int{}
	minCount := s.file.SegCount / s.config.NumSegments
	remainder := s.file.SegCount % s.config.NumSegments

	segment := 0
	minCounter := 1
	oldRemainder := remainder
	newRemainder := remainder

	for index := 0; index < s.file.SegCount; index++ {

		if minCounter == 1 {
			clusterPartition[segment] = []int{index}
		} else {
			clusterPartition[segment] = append(clusterPartition[segment], index)
		}

		if minCount == 0 {
			minCounter = 1
			segment++
			continue
		}

		if minCount <= minCounter {

			if remainder == 0 {
				minCounter = 1
				segment++
				continue
			}

			if newRemainder == 0 || newRemainder != oldRemainder {
				minCounter = 1
				segment++
				oldRemainder = newRemainder
				continue
			}
			newRemainder--
		}
		minCounter++

	}

	return clusterPartition
}

/////////////////////////////////////////////////////////////////////////////
// seeder goroutines

func (s *Seeder) Start() {
	// todo start future seeding goroutines here
	go s.automateStream()
}

func (s *Seeder) automateStream() {

	for {
		s.streamSegment()
		time.Sleep(time.Duration(s.config.SeedFrequency) * time.Millisecond)
	}
}

func (s *Seeder) streamSegment() {

	// find current least and most advanced segment
	mem := s.mlist.Members()
	if len(mem) < 2 {
		return
	}

	leastAdvancedSeg, mostAdvancedSeg := s.findLeastMostAdvancedSegment()

	// find random node from least advanced segment
	s.m.RLock()
	mem = s.segNumToNodes[leastAdvancedSeg]
	s.m.RUnlock()
	randNode := s.findRandomNode(mem)
	for randNode.Name == s.config.NodeName {
		randNode = s.findRandomNode(mem)
	}

	// find random segment part from most advanced segment
	segIdx, randSeg := s.findRandomSegment(mostAdvancedSeg)
	if segIdx == -1 {
		return
	}

	// sendSegment
	p2pMsg, err := message.CreateSegmentResponse(s.config.NodeName, segIdx, randSeg)
	if err != nil {
		return
	}
	s.sendSegment(randNode, p2pMsg)
	s.PrintString(fmt.Sprintf("[Stream Segment] || From: %s || To: %s || File Index: %d", s.config.NodeName, randNode.Name, segIdx))

}

func (s *Seeder) findLeastMostAdvancedSegment() (int, int) {

	s.m.RLock()
	defer s.m.RUnlock()

	leastAdvancedSeg := 10000
	mostAdvancedSeg := -1
	for segment, nodeList := range s.segNumToNodes {
		if len(nodeList) == 0 {
			continue
		}

		if segment != -1 && segment < leastAdvancedSeg {
			leastAdvancedSeg = segment
		}

		if segment > mostAdvancedSeg {
			mostAdvancedSeg = segment
		}
	}

	return leastAdvancedSeg, mostAdvancedSeg

}

/////////////////////////////////////////////////////////////////////////////
// streamer handle joining & leaving nodes

func (s *Seeder) NotifyJoin(node *memberlist.Node) {

	s.PrintString(fmt.Sprintf("[Seeder Notify Join] || Node: %s", node.Name))
	go s.handleNotifyJoin(node)

}

func (s *Seeder) handleNotifyJoin(node *memberlist.Node) {

	s.m.Lock()
	s.nodeToSegNum[node.Name] = 0
	s.segNumToNodes[0] = append(s.segNumToNodes[0], node)
	s.m.Unlock()
	go s.sendNodeClusterStatus(node)
	go s.sendAdvancedSegmentPart(node)
}

func (s *Seeder) sendAdvancedSegmentPart(node *memberlist.Node) {
	_, mostAdvancedSeg := s.findLeastMostAdvancedSegment()
	idx, randSeg := s.findRandomSegment(mostAdvancedSeg)
	if idx == -1 {
		return
	}
	p2p, err := message.CreateSegmentResponse(node.Name, idx, randSeg)
	if err != nil {
		return
	}
	s.sendSegment(node, p2p)
	s.PrintString(fmt.Sprintf("[ Sent Advanced ] || From: %s || To: %s || File Index: %d", s.config.NodeName, node.Name, idx))
}

func (s *Seeder) sendNodeClusterStatus(node *memberlist.Node) {
	s.m.RLock()
	msg := message.CreateNodeClusterStatusMsg(s.nodeToSegNum)
	s.m.RUnlock()
	p2pMsg, err := message.CreateP2PMsg(msg)
	if err != nil {
		return
	}
	p2pMsg.MsgType = message.NodeClusterStatusMsg
	s.sendSegment(node, p2pMsg)
	s.PrintString(fmt.Sprintf("[Sent NodeClusterStatus] || Node: %s", node.Name))
}

func (s *Seeder) NotifyLeave(node *memberlist.Node) {

	s.PrintString(fmt.Sprintf("[Seeder Notify Leave] || Node: %s", node.Name))
	go s.handleNotifyLeave(node)
}

func (s *Seeder) handleNotifyLeave(node *memberlist.Node) {
	s.m.Lock()
	defer s.m.Unlock()
	if segment, present := s.nodeToSegNum[node.Name]; present {

		// remove node from nodeToSegNum
		delete(s.nodeToSegNum, node.Name)

		// remove node from segNumToNodes
		segmentNodes := s.segNumToNodes[segment]
		for index, segNode := range segmentNodes {
			if segNode.Name == node.Name {
				s.segNumToNodes[segment] = append(segmentNodes[:index], segmentNodes[index+1:]...)
				break
			}
		}
	}
	s.PrintNodesSegment()
}

func (s *Seeder) NotifyUpdate(node *memberlist.Node) {
	s.PrintString(fmt.Sprintf("[Seeder Notify Update] || Node: %s", node.Name))
}

/////////////////////////////////////////////////////////////////////////////
// helper functions

func (s *Seeder) sendSegment(node *memberlist.Node, p2pMsg message.P2PMessage) {

	byt, err := message.EncodeMessage(p2pMsg)
	if err != nil {
		return
	}

	s.mlist.SendReliable(node, byt)

}

func (s *Seeder) findRandomSegment(segment int) (int, []byte) {

	if segment == s.config.NumSegments || segment < 0 {
		return -1, []byte{}
	}

	// find available partitions to retrieve from this segment
	s.m.RLock()
	availableSegmentPartition := s.filePartition[segment]
	s.m.RUnlock()

	// randomly choose from on of the available segment partitions
	numAvailableSegmentPartition := len(availableSegmentPartition)
	if numAvailableSegmentPartition == 0 {
		return -1, []byte{}
	}
	ss := rand.NewSource(time.Now().Unix())
	r := rand.New(ss)
	fileIdx := availableSegmentPartition[r.Intn(numAvailableSegmentPartition)]

	// retrieve file part from HD
	dirPath := fmt.Sprintf("%s/%s/", hls.SEED_HLS_PATH, s.file.NoExtFilename)
	cachedFilepath := dirPath + s.file.NoExtFilename + strconv.Itoa(fileIdx) + ".ts"
	data, err := ioutil.ReadFile(cachedFilepath)

	if err != nil {
		log.Println(err)
		return -1, []byte{}
	}

	return fileIdx, data
}

func (s *Seeder) findRandomNode(members []*memberlist.Node) *memberlist.Node {
	ss := rand.NewSource(time.Now().Unix())
	r := rand.New(ss) // initialize local pseudo random generator
	return members[r.Intn(len(members))]
}

func (s *Seeder) changeNodeCluster(nodeName string, segment int) {
	if segment == s.config.NumSegments {
		return
	}
	s.m.Lock()
	defer s.m.Unlock()

	// get nodeName's memberlist.Node
	nodeNode := &memberlist.Node{}
	for _, node := range s.mlist.Members() {
		if node.Name == nodeName {
			nodeNode = node
			break
		}
	}

	previousSegmentNum, present := s.nodeToSegNum[nodeName]
	if present {

		// if previous segment is the same as the given segment, no node segment change occurred
		if previousSegmentNum == segment {
			return
		}

		// remove node from prev segment
		prevSegmentNodes := s.segNumToNodes[previousSegmentNum]
		for index, node := range prevSegmentNodes {
			if node.Name == nodeName {
				s.segNumToNodes[previousSegmentNum] = append(prevSegmentNodes[:index], prevSegmentNodes[index+1:]...)
				break
			}
		}

	} else {

		// check segment already exists in segNumToNodes
		if _, segmentPresent := s.segNumToNodes[segment]; !segmentPresent {
			s.segNumToNodes[segment] = []*memberlist.Node{}
		}
	}

	// set node to new cluster
	s.segNumToNodes[segment] = append(s.segNumToNodes[segment], nodeNode)
	s.nodeToSegNum[nodeName] = segment

	log.Printf("%s changed segment number from %d to %d.\n", nodeName, previousSegmentNum, segment)
	s.PrintNodesSegment()
}

/////////////////////////////////////////////////////////////////////////////
// handle rsp

// Delegate protocol implementation for Seeder
func (s *Seeder) NodeMeta(limit int) []byte {
	return []byte{}
}

func (s *Seeder) NotifyMsg(b []byte) {
	// all messages sent/broadcast must be wrapped into P2PMessage
	msg, err := message.DecodeP2PMessage(b)

	if err != nil {
		log.Println(err)
		return
	}

	go s.handleMsg(msg)
}

func (s *Seeder) GetBroadcasts(overhead, limit int) [][]byte {
	return s.broadcasts.GetBroadcasts(overhead, limit)
}

func (s *Seeder) LocalState(join bool) []byte {
	//stub, we don't sync state
	return []byte{}
}

func (s *Seeder) MergeRemoteState(buf []byte, join bool) {
	//stub, we don't sync state
}

func (s *Seeder) handleMsg(msg message.P2PMessage) {

	switch msg.MsgType {
	case message.ClusterChangeMsg:
		changeCluster, err := message.DecodeClusterChange(msg.Payload)

		if err != nil {
			log.Println(err)
			return
		}

		s.PrintString(fmt.Sprintf("[Received ClusterNodeChange] Node: %s %s %d", changeCluster.NodeName, "Cluster:", changeCluster.Cluster))
		s.changeNodeCluster(changeCluster.NodeName, changeCluster.Cluster)

	default:
		log.Println("Unhandled message type in Seeder:", msg.MsgType)
	}
}

/////////////////////////////////////////////////////////////////////////////
// handle rsp

func (s *Seeder) PrintNodesSegment() {
	for node, cluster := range s.nodeToSegNum {
		log.Println("[PrintNodesSegment] || Node: ", node, "|| Cluster:", strconv.Itoa(int(cluster)))
	}
}

func (s *Seeder) PrintMembers() {
	log.Println()
	log.Println("Member List:")
	for _, mem := range s.mlist.Members() {
		log.Println(mem.Name)
	}
}

func (s *Seeder) PrintString(msg string) {
	log.Println()
	log.Println(msg)
}
