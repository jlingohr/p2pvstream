# P2P on demand video streaming based on tit-for-tat strategy

Peer-to-peer streaming client. Allows a client to join a peer-to-peer network. Upon joining gossiping protocol is used to discover content available to stream in the network and also specify what content the client is willing to upload into the network. 

For full details see the [proposal](https://github.com/jlingohr/p2pvstream/blob/master/report/proposal.pdf) and the final [report](https://github.com/jlingohr/p2pvstream/blob/master/report/report.pdf). 

##### Dependencies:

* Gorilla Mux
    ```
    > go get -u github.com/gorilla/mux
    ```
* ffmpeg
    ```
    > brew update
    > brew install
    ```
`go run main.go`

Runs main client using settings in a config.json file with the following parameters:

* string NodeName: Name of current node
* string ClusterMemberAddress: IP address and port of current node
* string ClusterBindAddress": IP address and port of known peer in gossip network 
* string SeedBindAddress: IP address and port peers bind to in order to stream from current node
* string StreamBindAddress: IP address and port to use in order to stream from others
* string HTTPBindPort: IP address and port to view stream. This is for testing purposes
* string SeedFilename: Video file others can stream 
* int NodeRetransmitMux: Number of retries in peer gossip network before notification
* int NumSegments: Number of partitions we break video file into
* int SeedFrequency: How often to upload chunk of file to leeching swarm when seeding
* int ExchangeFrequency: How often to exchange chunks of a file with the swarm when leeching
* int ClusterRetransmitMux: Number of retries in swarm before notification

