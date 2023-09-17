package main

import (
	"fmt"
	"time"
)

type Node struct {
	isOffline      chan bool
	id             int
	maxOutStanding int
	processFunc    func(int) (int, error)
	parentCls      *Cluster
}

func (node *Node) Serve(reqChan chan *Request, resChan chan<- *Request) {
	fmt.Printf("[%d] node: ✅ Started...\n", node.id)
	sema := make(chan int, node.maxOutStanding)
	done := make(chan bool)
	go node.sendHeartbeatToParentCluster(done)
	for req := range reqChan {
		select {
		case <-node.isOffline:
			fmt.Printf("[%d] node killed 🛑\n", node.id)
			// broadcast it to other nodes
			// otherwise our req will be lost
			go func(req *Request) {
				resChan <- req
			}(req)
			done <- true
			return // Node killed/down
		default:
			req := req
			fmt.Printf("node %d: reqId:%v Waiting...[%s]\n", node.id, (*req).id, time.Now().Format("2006-01-02 15:04:05"))
			sema <- 1 // acquire semaphore
			go func() {
				node.handle(req, resChan)
				<-sema // release semaphore after handle is done
			}()
		}

	}
	done <- true // reqChan is closed, close any spawned child routine of this node instance
}
func (node *Node) handle(newReq *Request, resChan chan<- *Request) {
	fmt.Printf("[%d] handle: reqId:%v Inside...[%s]\n", node.id, (*newReq).id, time.Now().Format("2006-01-02 15:04:05"))
	// blocking process
	if val, err := node.processFunc(newReq.num); err != nil {
		go node.responseEmitter(newReq, fmt.Sprintf("ERROR %s", err.Error()), resChan)
	} else {
		newReq.num = val
		go node.responseEmitter(newReq, "Node response 200", resChan) // send response using seperate go routine
	}
	fmt.Printf("handle: reqId:%v COMPLETED...[%s]\n", (*newReq).id, time.Now().Format("2006-01-02 15:04:05"))
}
func (node *Node) responseEmitter(res *Request, msg string, resChan chan<- *Request) {
	fmt.Printf("[%d] responseEmitter: resId:%v [%s]\n", node.id, (*res).id, time.Now().Format("2006-01-02 15:04:05"))
	res.msg = msg
	resChan <- res
}
func (node *Node) sendHeartbeatToParentCluster(done <-chan bool) {
	tickChan := time.Tick(500 * time.Millisecond)
	for {
		select {
		case <-tickChan:
			// send heartbeat every tick
			node.parentCls.heartbeatChannel <- node.id // buffered channel
		case <-done:
			fmt.Printf("[%d] node stopped sending heartbeat\n", node.id)
			return // exit the routine
		}
	}
}
func createNewNode(parentCls *Cluster, id int, maxOutStanding int, processFunc func(int) (int, error)) *Node {
	return &Node{
		isOffline:      make(chan bool),
		parentCls:      parentCls,
		id:             id,
		maxOutStanding: maxOutStanding,
		processFunc:    processFunc,
	}
}
