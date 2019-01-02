package p2p

import (
	"errors"
	"fmt"
	"github.com/hashicorp/memberlist"
	"github.com/jlingohr/p2pvstream/hls"
	"github.com/jlingohr/p2pvstream/message"
	"github.com/jlingohr/p2pvstream/settings"
	"github.com/jlingohr/p2pvstream/streaming"
	"io/ioutil"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"
)


type Streamer struct {
	file               streaming.StreamedFile
	mlist              *memberlist.Memberlist
	broadcast          *memberlist.TransmitLimitedQueue
	nodeToSegNum       map[string]int             // Node name to segment number map
	segNumToNodes      map[int][]*memberlist.Node // Segment number to nodes map
	streamedPartitions map[int]bool               // key: file index value: is file partition retrieved?
	filePartitions     map[int][]int              // key: segment value: array of file indexes
	config             settings.Config
	m                  sync.RWMutex
	stopStreaming      chan struct{}
}

type streamerConfig struct {
	seederAddr string
	seederName string
	streamedFile streaming.StreamedFile
	global settings.Config
}

func NewStreamer(config streamerConfig) (*Streamer, error) {
	addrParts := strings.Split(config.global.StreamBindAddress, ":")

	if len(addrParts) != 2 {
		log.Println("Invalid address supplied")
		return &Streamer{}, errors.New("invalid bind address supplied")
	}
	bindAddr := addrParts[0]
	bindPort := addrParts[1]

	c := memberlist.DefaultWANConfig()
	c.BindPort, _ = strconv.Atoi(bindPort)
	c.BindAddr = bindAddr

	// todo we might want to use push pull here to sync the state across the nodes
	// todo it would also be a good idea to use it to initially sync the state with the cluster
	// todo then we also won't need seederName here, could get from initial sync
	c.PushPullInterval = 0 // disable push/pull

	c.Name = config.global.NodeName

	if config.global.ClusterGossipInterval != 0 {
		c.GossipInterval = time.Duration(config.global.ClusterGossipInterval) * time.Millisecond
	}

	if config.global.ClusterProbeInterval != 0 {
		c.ProbeInterval = time.Duration(config.global.ClusterProbeInterval) * time.Millisecond
	}

	if config.global.ClusterProbeTimeout != 0 {
		c.ProbeTimeout = time.Duration(config.global.ClusterProbeTimeout) * time.Millisecond
	}

	if config.global.ClusterSuspicionMult != 0 {
		c.SuspicionMult = config.global.ClusterSuspicionMult
	}

	m, err := memberlist.Create(c)

	if err != nil {
		return &Streamer{}, err
	}

	if _, err := m.Join([]string{config.seederAddr}); err != nil {
		log.Println(err)
		return &Streamer{}, err
	}

	b := &memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			return m.NumMembers()
		},
		RetransmitMult: config.global.ClusterRetransmitMux,
	}

	seederNode := &memberlist.Node{}
	nodeNode := &memberlist.Node{}
	for _, member := range m.Members() {
		if member.Name == config.seederName {
			seederNode = member
		} else if member.Name == config.global.NodeName {
			nodeNode = member
		}
	}

	nodeToSegNum := map[string]int{config.seederName: -1, config.global.NodeName: 0}
	SegNumToNodes := map[int][]*memberlist.Node{}
	SegNumToNodes[-1] = []*memberlist.Node{seederNode}
	SegNumToNodes[0] = []*memberlist.Node{nodeNode}
	video := map[int]bool{}
	fileIdx := make([]int, config.streamedFile.SegCount)
	for index, _ := range fileIdx {
		video[index] = false
	}

	streamer := &Streamer{
		file:               config.streamedFile,
		mlist:              m,
		broadcast:          b,
		nodeToSegNum:       nodeToSegNum,
		segNumToNodes:      SegNumToNodes,
		config:             config.global,
		streamedPartitions: video,
		m:                  sync.RWMutex{},
		stopStreaming: make(chan struct{}),
	}
	streamer.filePartitions = streamer.getFilePartition()

	c.Delegate = streamer
	c.Events = streamer

	log.Println("File Partition:", streamer.filePartitions)

	return streamer, nil
}

func (s *Streamer) getFilePartition() map[int][]int {

	clusterPartition := map[int][]int{}
	minCount := s.file.SegCount / s.config.NumSegments
	remainder := s.file.SegCount % s.config.NumSegments

	segment := 0
	minCounter := 1
	oldRemainder := remainder
	newRemainder := remainder
	for index, _ := range make([]int, s.file.SegCount) {

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
// streamer goroutines

func (s *Streamer) Start(stop chan struct{}) {
	// start streaming routines
	s.stopStreaming = make(chan struct{})

	if s.file.SegCount == 0 {
		s.exitStream()
		return
	}

	go s.startInternalSegmentExchange()
	go s.startNeighbouringSegmentExchange()

	go func() {
		select {
		case <-stop:
			s.exitStream()
		case <-s.stopStreaming:
			close(stop)
			return
		}
	}()
}

func (s *Streamer) startInternalSegmentExchange() {
	ticker := time.NewTicker(time.Duration(s.config.ExchangeFrequency) * time.Millisecond).C

	for {
		select {
		case <-s.stopStreaming:
			s.PrintString("[Exiting InternalSegmentExchange]")
			return
		case <-ticker:
			s.exchangeInternalSegment()
		}
	}
}

func (s *Streamer) startNeighbouringSegmentExchange() {
	ticker := time.NewTicker(time.Duration(s.config.ExchangeFrequency) * time.Millisecond).C

	for {

		select {
		case <-s.stopStreaming:
			s.PrintString("[Exiting NeighbouringSegmentExchange]")
			return
		case <-ticker:
			s.exchangeNeighbouringSegment()
		}
	}
}

func (s *Streamer) exchangeInternalSegment() {

	// find random node of the same segment
	s.m.RLock()
	segment := s.nodeToSegNum[s.config.NodeName]
	mem := s.segNumToNodes[segment]
	s.m.RUnlock()

	if len(mem) < 2 {
		return
	}
	randNode := s.findRandomNode(mem)
	for randNode.Name == s.config.NodeName {
		randNode = s.findRandomNode(mem)
	}

	// find random segment part of the same segment
	segIdx, randSeg := s.findRandomSegment(segment)
	if segIdx == -1 {
		return
	}

	// sendSegment
	p2pMsg, err := message.CreateSegmentRequest(s.config.NodeName, segIdx, randSeg)
	if err != nil {
		return
	}

	s.sendSegment(randNode, p2pMsg)
	s.PrintString(fmt.Sprintf("[Sent InternalSegmentExchange] || From: %s %s %s %s %s", s.config.NodeName, "|| To:", randNode.Name, "|| File Index:", strconv.Itoa(segIdx)))

}

func (s *Streamer) exchangeNeighbouringSegment() {

	// find current segment
	s.m.RLock()
	segment := s.nodeToSegNum[s.config.NodeName]

	// find random neighbouring node (next occupied segment)
	mem := make([]*memberlist.Node, 0)
	for len(mem) == 0 {
		segment++
		if segment == s.config.NumSegments {
			return
		}
		mem = s.segNumToNodes[segment]
	}
	randNode := s.findRandomNode(mem)

	// find a random piece from the most advanced segment
	advancedSegment := segment
	acc := advancedSegment + 1
	for acc != s.config.NumSegments {
		mem = s.segNumToNodes[acc]
		if len(mem) != 0 {
			advancedSegment = acc
		}
		acc++
	}
	s.m.RUnlock()

	segIdx, randSeg := s.findRandomSegment(advancedSegment)
	if segIdx == -1 {
		return
	}

	// sendSegment
	p2pMsg, err := message.CreateSegmentRequest(s.config.NodeName, segIdx, randSeg)
	if err != nil {
		return
	}
	s.sendSegment(randNode, p2pMsg)
	s.PrintString(fmt.Sprintf("[Sent NeighbouringSegmentExchange] || From: %s %s %s %s %s", s.config.NodeName, "|| To:", randNode.Name, "|| File Index:", strconv.Itoa(segIdx)))

}

/////////////////////////////////////////////////////////////////////////////
// streamer handle joining & leaving nodes

func (s *Streamer) NotifyJoin(node *memberlist.Node) {

	s.PrintString(fmt.Sprintf("[Streamer Notify Join] || Node: %s", node.Name))
	go s.handleNotifyJoin(node)
}

func (s *Streamer) NotifyLeave(node *memberlist.Node) {

	s.PrintString(fmt.Sprintf("[Streamer Notify Leave] || Node: %s", node.Name))
	go s.handleNotifyLeave(node)
}

func (s *Streamer) NotifyUpdate(node *memberlist.Node) {
	s.PrintString(fmt.Sprintf("[Streamer Notify Update] || Node: %s", node.Name))
}

func (s *Streamer) handleNotifyJoin(node *memberlist.Node) {
	s.m.Lock()
	s.nodeToSegNum[node.Name] = 0
	s.segNumToNodes[0] = append(s.segNumToNodes[0], node)
	s.m.Unlock()

	s.m.RLock()
	s.PrintNodesSegment()
	s.m.RUnlock()
}

func (s *Streamer) handleNotifyLeave(node *memberlist.Node) {

	s.m.RLock()
	nodeCluster := s.nodeToSegNum[node.Name]
	s.m.RUnlock()

	if nodeCluster == -1 {
		// TODO: must find another node to seed from otherwise, streaming ends
		s.PrintString(fmt.Sprintf("Seeder is DEAD!!!!!!!!!! Seeder is DEAD!!!!!!!!!! Seeder is DEAD!!!!!!!!!!"))
		s.exitStream()
		return
	}

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

/////////////////////////////////////////////////////////////////////////////
// streamer handle rsp
/////////////////////////////////////////////////////////////////////////////

//////////////////////////////////////////////////////////////////////////////
// streamer delegate rsp methods

func (s *Streamer) NodeMeta(limit int) []byte {
	return []byte{}
}

func (s *Streamer) GetBroadcasts(overhead, limit int) [][]byte {
	return s.broadcast.GetBroadcasts(overhead, limit)
}

func (s *Streamer) LocalState(join bool) []byte {
	//stub, we don't sync state
	return []byte{}
}

func (s *Streamer) MergeRemoteState(buf []byte, join bool) {
	//stub, we don't sync state
}

func (s *Streamer) NotifyMsg(b []byte) {
	// all messages sent/broadcast must be wrapped into P2PMessage
	msg, err := message.DecodeP2PMessage(b)

	if err != nil {
		log.Println(err)
		return
	}

	go s.handleMsg(msg)
}

//////////////////////////////////////////////////////////////////////////////
// streamer rsp methods

func (s *Streamer) handleMsg(msg message.P2PMessage) {

	switch msg.MsgType {

	case message.NodeClusterStatusMsg:

		segChange, err := message.DecodeNodeClusterStatus(msg.Payload)

		if err != nil {
			log.Println(err)
			return
		}

		log.Println()
		for _, clusterChange := range segChange.NodeClusterList {
			s.PrintString(fmt.Sprintf("[Received NodeClusterStatus] || Node: %s %s %d", clusterChange.NodeName, "|| Cluster:", clusterChange.Cluster))
			s.changeNodeCluster(clusterChange.NodeName, clusterChange.Cluster)
		}

	case message.ClusterChangeMsg:

		segChange, err := message.DecodeClusterChange(msg.Payload)

		if err != nil {
			log.Println(err)
			return
		}

		s.PrintString(fmt.Sprintf("[Received ClusterNodeChange] || Node: %s %s %d", segChange.NodeName, "|| Cluster:", segChange.Cluster))
		s.changeNodeCluster(segChange.NodeName, segChange.Cluster)

	case message.ExchangeSegmentRequestMsg:

		tradeSeg, err := message.DecodeTradeSegmentSegment(msg.Payload)

		if err != nil {
			log.Println(err)
			return
		}

		s.PrintString(fmt.Sprintf("[Received ExchangeSegmentRequest] || From: %s %s %s %s %d", tradeSeg.NodeName, "|| To:", s.config.NodeName, "|| Idx:", tradeSeg.Idx))
		s.handleSegmentRequest(tradeSeg)

	case message.ExchangeSegmentResponseMsg:

		tradeSeg, err := message.DecodeTradeSegmentSegment(msg.Payload)

		if err != nil {
			log.Println(err)
			return
		}

		s.PrintString(fmt.Sprintf("[Received ExchangeSegmentResponse] || From: %s %s %s %s %d", tradeSeg.NodeName, "|| To:", s.config.NodeName, "|| Idx:", tradeSeg.Idx))
		s.handleSegmentResponse(tradeSeg)

	default:
		log.Println("Unhandled message type in Streamer:", msg.MsgType)
	}
}

func (s *Streamer) changeNodeCluster(nodeName string, segment int) {
	if segment >= s.config.NumSegments || segment <= -1 {
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
		if _, segMentPresent := s.segNumToNodes[segment]; !segMentPresent {
			s.segNumToNodes[segment] = []*memberlist.Node{}
		}
	}

	// set node to new cluster
	s.segNumToNodes[segment] = append(s.segNumToNodes[segment], nodeNode)
	s.nodeToSegNum[nodeName] = segment

	log.Printf("%s changed segment number from %d to %d.\n", nodeName, previousSegmentNum, segment)
	s.PrintNodesSegment()
}

func (s *Streamer) broadcastClusterChange(segment int) {
	announceMsg := message.ClusterChange{
		NodeName: s.config.NodeName,
		Cluster:  segment,
	}

	encodedMsg, err := message.EncodeMessage(announceMsg)

	if err != nil {
		log.Println(err)
		return
	}

	p2pMessage := message.P2PMessage{
		MsgType: message.ClusterChangeMsg,
		Payload: encodedMsg,
	}

	s.broadcast.QueueBroadcast(message.BroadcastMsg{Msg: p2pMessage})
}

func (s *Streamer) handleSegmentRequest(msg message.Segment) {

	s.saveSegment(msg)
	s.sendSegmentResponse(msg)

}

func (s *Streamer) sendSegmentResponse(msg message.Segment) {

	// find if node exists
	var toNode *memberlist.Node
	for _, node := range s.mlist.Members() {
		if node.Name == msg.NodeName {
			toNode = node
			break
		}
	}
	if toNode == nil {
		return
	}

	// find toNode's segment
	s.m.RLock()
	segment := s.nodeToSegNum[toNode.Name]
	s.m.RUnlock()

	// find random segment part from segment
	segIdx, randSeg := s.findRandomSegment(segment)
	if segIdx == -1 {
		return
	}

	// send Segment
	p2pMsg, err := message.CreateSegmentResponse(s.config.NodeName, segIdx, randSeg)
	if err != nil {
		return
	}
	s.sendSegment(toNode, p2pMsg)
	s.PrintString(fmt.Sprintf("[Sent ExchangeSegmentResponse] || From: %s %s %s %s %d", s.config.NodeName, "|| To:", toNode.Name, "|| Idx:", segIdx))

}

func (s *Streamer) handleSegmentResponse(tradeSeg message.Segment) {

	s.saveSegment(tradeSeg)

	// check if segment is complete
	if !s.isCurrentSegmentComplete() {
		return
	}

	// send the given piece internally before changing segments
	s.sendSegmentPartitionBeforeLeaving(tradeSeg)

	// change segment
	s.m.RLock()
	segment := s.nodeToSegNum[s.config.NodeName]
	isCurrentLastSegment := segment+1 == s.config.NumSegments
	nextSegmentPartition := make([]int, 0)
	if partition, present := s.filePartitions[segment+1]; present {
		nextSegmentPartition = partition
	}
	s.m.RUnlock()

	if isCurrentLastSegment || len(nextSegmentPartition) == 0 {
		s.PrintString(fmt.Sprintf("[[ File Streaming is complete. ]] "))
		s.exitStream()

	} else {
		s.changeCurrentNodeCluster(segment + 1)
	}

}

func (s *Streamer) saveSegment(seg message.Segment) {

	s.m.RLock()
	videoPres := s.streamedPartitions[seg.Idx]
	s.m.RUnlock()
	if videoPres {
		return
	}

	// save file
	dirPath := fmt.Sprintf("%s%s/%s/", hls.CACHE_PATH, s.config.NodeName, s.file.NoExtFilename)
	cachedFilepath := dirPath + "/" + s.file.NoExtFilename + strconv.Itoa(seg.Idx) + ".ts"
	err := ioutil.WriteFile(cachedFilepath, seg.Segment, 0644)

	if err != nil {
		log.Println(err)
		return
	}

	// update streamedPartitions file partition map
	s.m.Lock()
	s.streamedPartitions[seg.Idx] = true
	s.m.Unlock()

	s.PrintString(fmt.Sprintf("[Saved Segment] || Idx: %d", seg.Idx))

}

func (s *Streamer) sendSegmentPartitionBeforeLeaving(tradeSeg message.Segment) {

	s.m.RLock()
	segment := s.nodeToSegNum[s.config.NodeName]
	mem := s.segNumToNodes[segment]
	s.m.RUnlock()

	if len(mem) > 1 {

		randNode := s.findRandomNode(mem)
		for randNode.Name == s.config.NodeName {
			randNode = s.findRandomNode(mem)
		}

		p2pMsg, err := message.CreateP2PMsg(tradeSeg)
		if err == nil {
			p2pMsg.MsgType = message.ExchangeSegmentResponseMsg
			s.sendSegment(randNode, p2pMsg)
			s.PrintString(fmt.Sprintf("[Sent ExchangeSegmentResponse] || From: %s %s %s %s %d", s.config.NodeName, "|| To:", randNode.Name, "|| Idx:", tradeSeg.Idx))

		}
	}
}

func (s *Streamer) exitStream() {
	s.PrintString(fmt.Sprintf("[[ Exiting Stream. ]]"))
	s.mlist.Leave(time.Second)
	time.Sleep(time.Second)
	close(s.stopStreaming)
	s.mlist.Shutdown()
	s.PrintString(fmt.Sprintf("[[ Exit Stream Complete. ]]"))
}

func (s *Streamer) changeCurrentNodeCluster(segment int) {
	s.changeNodeCluster(s.config.NodeName, segment)
	s.broadcastClusterChange(segment)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////
// helper functions

func (s *Streamer) findRandomNode(members []*memberlist.Node) *memberlist.Node {
	ss := rand.NewSource(time.Now().Unix())
	r := rand.New(ss) // initialize local pseudo random generator
	return members[r.Intn(len(members))]
}

func (s *Streamer) findRandomSegment(segment int) (int, []byte) {

	if segment >= s.config.NumSegments || segment < 0 {
		return -1, []byte{}
	}

	// find available partitions to retrieve from this segment
	s.m.RLock()
	segmentPartition := s.filePartitions[segment]
	availableSegmentParitions := []int{}
	for _, segPart := range segmentPartition {
		if s.streamedPartitions[segPart] {
			availableSegmentParitions = append(availableSegmentParitions, segPart)
		}
	}
	s.m.RUnlock()

	// randomly choose from on of the available segment partitions
	numAvailableSegmentPartition := len(availableSegmentParitions)
	if numAvailableSegmentPartition == 0 {
		return -1, []byte{}
	}
	ss := rand.NewSource(time.Now().Unix())
	r := rand.New(ss)
	fileIdx := availableSegmentParitions[r.Intn(numAvailableSegmentPartition)]

	// retrieve file part from HD
	dirPath := fmt.Sprintf("%s%s/%s/", hls.CACHE_PATH, s.config.NodeName, s.file.NoExtFilename)
	cachedFilepath := dirPath + "/" + s.file.NoExtFilename + strconv.Itoa(fileIdx) + ".ts"
	data, err := ioutil.ReadFile(cachedFilepath)

	if err != nil {
		log.Println(err)
		return -1, []byte{}
	}

	return fileIdx, data

}

func (s *Streamer) sendSegment(node *memberlist.Node, p2pMsg message.P2PMessage) {

	byt, err := message.EncodeMessage(p2pMsg)
	if err != nil {
		return
	}

	s.mlist.SendReliable(node, byt)

}

func (s *Streamer) isCurrentSegmentComplete() bool {

	s.m.RLock()
	defer s.m.RUnlock()

	segment := s.nodeToSegNum[s.config.NodeName]
	segmentPartition := s.filePartitions[segment]

	for _, partitionSaved := range segmentPartition {
		segPartitionComplete := s.streamedPartitions[partitionSaved]
		if !segPartitionComplete {
			return false
		}
	}
	return true
}

////////////////////////////////////////////////////////////////////////////////////////////////////////
// print

func (s *Streamer) PrintNodesSegment() {
	for node, cluster := range s.nodeToSegNum {
		log.Println("[PrintNodesSegment] || Node:", node, "|| Cluster:", cluster)
	}
}

func (s *Streamer) PrintMembers() {
	log.Println()
	log.Println("Member List:")
	for _, mem := range s.mlist.Members() {
		log.Println(mem.Name)
	}
}

func (s *Streamer) PrintSavedSegments() {
	s.m.RLock()
	defer s.m.RUnlock()
	log.Println("Saved Segments:")
	for idx, isSaved := range s.streamedPartitions {
		log.Println("File Idx:", idx, "Streamed:", isSaved)
	}
}

func (s *Streamer) PrintString(msg string) {
	log.Printf("\n%s\n", msg)
}
