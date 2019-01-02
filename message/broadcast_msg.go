package message

import (
	"bytes"
	"encoding/gob"
	"github.com/hashicorp/memberlist"
	"log"
)

type BroadcastMsg struct {
	Msg P2PMessage
}

func (bm BroadcastMsg) Finished() {
}

func (bm BroadcastMsg) Invalidates(b memberlist.Broadcast) bool {
	// todo figure out what that does
	return false
}

func (bm BroadcastMsg) Message() []byte {
	msg, err := EncodeMessage(bm.Msg)

	if err != nil {
		log.Println(err)
		return []byte{}
	}

	return msg
}

// Broadcast message Payload types
type P2PMessageType int

const (
	SeedAnnounceMsg            P2PMessageType = iota
	MetadataRequestMsg         P2PMessageType = iota
	MetadataResponseMsg        P2PMessageType = iota
	AnnounceSegmentMsg         P2PMessageType = iota
	ExchangeSegmentRequestMsg  P2PMessageType = iota
	ExchangeSegmentResponseMsg P2PMessageType = iota
	ClusterChangeMsg           P2PMessageType = iota
	NodeClusterStatusMsg       P2PMessageType = iota
)

type P2PMessage struct {
	MsgType P2PMessageType
	Payload []byte
}

type SeedFileAnnounce struct {
	Filename  string
	NodeName  string
	Timestamp int64
}

type AnnounceSegment struct {
	NodeName   string
	SegmentNum int
}

type Segment struct {
	NodeName string
	Idx      int
	Segment  []byte
}

type ClusterChange struct {
	NodeName string
	Cluster  int
}

type NodeClusterStatus struct {
	NodeClusterList []ClusterChange
}

func EncodeMessage(msg interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(msg)
	return buf.Bytes(), err
}

func DecodeP2PMessage(b []byte) (P2PMessage, error) {
	var msg P2PMessage
	bufDecoder := bytes.NewBuffer(b)
	decoder := gob.NewDecoder(bufDecoder)
	err := decoder.Decode(&msg)
	if err != nil {
		return P2PMessage{}, err
	}

	return msg, nil
}

func DecodeSeedAnnounce(b []byte) (SeedFileAnnounce, error) {
	var msg SeedFileAnnounce
	bufDecoder := bytes.NewBuffer(b)
	decoder := gob.NewDecoder(bufDecoder)
	err := decoder.Decode(&msg)
	if err != nil {
		return SeedFileAnnounce{}, err
	}

	return msg, nil
}

func DecodeAnnounceSegment(b []byte) (AnnounceSegment, error) {
	var msg AnnounceSegment
	bufDecoder := bytes.NewBuffer(b)
	decoder := gob.NewDecoder(bufDecoder)
	err := decoder.Decode(&msg)
	if err != nil {
		return AnnounceSegment{}, err
	}

	return msg, nil
}

func DecodeTradeSegmentSegment(b []byte) (Segment, error) {
	var msg Segment
	bufDecoder := bytes.NewBuffer(b)
	decoder := gob.NewDecoder(bufDecoder)
	err := decoder.Decode(&msg)
	if err != nil {
		return Segment{}, err
	}

	return msg, nil
}

func DecodeClusterChange(b []byte) (ClusterChange, error) {
	var msg ClusterChange
	bufDecoder := bytes.NewBuffer(b)
	decoder := gob.NewDecoder(bufDecoder)
	err := decoder.Decode(&msg)
	if err != nil {
		return ClusterChange{}, err
	}

	return msg, nil
}

func DecodeNodeClusterStatus(b []byte) (NodeClusterStatus, error) {
	var msg NodeClusterStatus
	bufDecoder := bytes.NewBuffer(b)
	decoder := gob.NewDecoder(bufDecoder)
	err := decoder.Decode(&msg)
	if err != nil {
		return NodeClusterStatus{}, err
	}

	return msg, nil
}

func CreateSegment(nodeName string, idx int, payload []byte) Segment {

	return Segment{
		NodeName: nodeName,
		Idx:      idx,
		Segment:  payload,
	}
}

func CreateP2PMsg(msg interface{}) (P2PMessage, error) {

	byt, err := EncodeMessage(msg)
	if err != nil {
		return P2PMessage{}, err
	}

	return P2PMessage{
		Payload: byt,
	}, nil

}

func CreateNodeClusterStatusMsg(nodeToSegNum map[string]int) NodeClusterStatus {

	clusterStatus := []ClusterChange{}
	for node, segment := range nodeToSegNum {
		announceMsg := ClusterChange{
			NodeName: node,
			Cluster:  segment,
		}
		clusterStatus = append(clusterStatus, announceMsg)
	}

	return NodeClusterStatus{
		NodeClusterList: clusterStatus,
	}

}

func CreateSegmentRequest(nodeName string, idx int, payload []byte) (P2PMessage, error) {

	segment := CreateSegment(nodeName, idx, payload)
	p2pMsg, err := CreateP2PMsg(segment)
	if err != nil {
		return P2PMessage{}, err
	}
	p2pMsg.MsgType = ExchangeSegmentRequestMsg
	return p2pMsg, err
}

func CreateSegmentResponse(nodeName string, idx int, payload []byte) (P2PMessage, error) {

	segment := CreateSegment(nodeName, idx, payload)
	p2pMsg, err := CreateP2PMsg(segment)
	if err != nil {
		return P2PMessage{}, err
	}
	p2pMsg.MsgType = ExchangeSegmentResponseMsg
	return p2pMsg, err
}
