package message

import (
	"bytes"
	"encoding/gob"
)

type MetadataRequest struct {
	OriginNodeName string
	Filename       string // just to make sure we are requesting the correct file
}

type MetadataResponse struct {
	NodeName     string
	NodeAddr     string
	MetaFilename string
	File         []byte
	NumOfParts   int
}

func DecodeMetadataRequest(b []byte) (MetadataRequest, error) {
	var msg MetadataRequest
	bufDecoder := bytes.NewBuffer(b)
	decoder := gob.NewDecoder(bufDecoder)
	err := decoder.Decode(&msg)
	if err != nil {
		return MetadataRequest{}, err
	}

	return msg, nil
}

func DecodeMetadataResponse(b []byte) (MetadataResponse, error) {
	var msg MetadataResponse
	bufDecoder := bytes.NewBuffer(b)
	decoder := gob.NewDecoder(bufDecoder)
	err := decoder.Decode(&msg)
	if err != nil {
		return MetadataResponse{}, err
	}

	return msg, nil
}
