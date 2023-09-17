package main

import (
	"fmt"
	"sync"
	"time"
)

type Cluster struct {
	servers               []*Node
	size                  int
	maxOutStandingPerNode int
	heartbeatChannel      chan int
	heartbeatMap          map[int]time.Time
}

var nodeProcessFunc func(int) (int, error)
var nodeReqChan chan *Request
var nodeResChan chan<- *Request
var mtx sync.Mutex

func (cls *Cluster) Start(reqChan chan *Request, resChan chan<- *Request) {
	nodeReqChan = reqChan
	nodeResChan = resChan
	fmt.Println("spawning nodes...")
	for i := 0; i < cls.size; i++ {
		serverIns := cls.servers[i]
		go serverIns.Serve(reqChan, resChan)
	}
	fmt.Println("spawn nodes completed")
	// start heartbeat analysis
	go func() {
		time.Sleep(1 * time.Second) // after some time start monitoring system
		go cls.recordHeartbeats()
		go cls.monitorHeartbeats()
		fmt.Println("Node monitoring system started... 💺")
	}()
}
func (cls *Cluster) KillNode(id int) {
	// index = id
	serverIns := cls.servers[id]
	go func() {
		serverIns.isOffline <- true // kill the node
	}()
}
func (cls *Cluster) recordHeartbeats() {
	for {
		select {
		case nodeId := <-cls.heartbeatChannel:
			// store recent beats from nodes
			mtx.Lock()
			cls.heartbeatMap[nodeId] = time.Now()
			mtx.Unlock()
		}
	}
}
func (cls *Cluster) monitorHeartbeats() {
	// monitor every 1sec
	tick := time.Tick(1 * time.Second)
	for range tick { // for every tick
		for nodeId := 0; nodeId < cls.size; nodeId++ {
			mtx.Lock()
			// [0-size)
			if lastRcvdBeat, exists := cls.heartbeatMap[nodeId]; !exists {
				// didnot received heartbeat from nodeid
				fmt.Printf("No heartbeat ☠️ found for node [%d]... Creating and Restarting.\n", nodeId)
				cls.servers[nodeId] = createNewNode(cls, nodeId, cls.maxOutStandingPerNode, nodeProcessFunc)
				go cls.servers[nodeId].Serve(nodeReqChan, nodeResChan)
			} else {
				elapsed := time.Since(lastRcvdBeat)
				fmt.Printf("last elapsed beat for node [%d]: %0.3f s\n", nodeId, elapsed.Seconds())
				if elapsed.Seconds() > 5 {
					// if last recvd beat is greated than 5s, restart the node
					fmt.Printf("❌ Expired heartbeat for node [%d] (%.3f s)... Restarting.\n", nodeId, elapsed.Seconds())
					cls.heartbeatMap[nodeId] = time.Now()
					go cls.servers[nodeId].Serve(nodeReqChan, nodeResChan)
				}
			}
			mtx.Unlock()
		}
	}
}
func CreateNewCluster(processFunc func(int) (int, error), size, maxOutStandingPerNode int) *Cluster {
	// create cluster object
	cls := &Cluster{
		size:                  size,
		maxOutStandingPerNode: maxOutStandingPerNode,
		heartbeatChannel:      make(chan int, size), // buffered channel to avoid potential delay
		heartbeatMap:          make(map[int]time.Time),
	}
	// create servers
	servers := make([]*Node, size)
	fmt.Println("Creating nodes.....")
	nodeProcessFunc = processFunc
	for i := 0; i < size; i++ {
		// id = i
		servers[i] = createNewNode(cls, i, maxOutStandingPerNode, processFunc)
	}
	fmt.Println("cluster created.")
	// update servers
	cls.servers = servers
	return cls
}
