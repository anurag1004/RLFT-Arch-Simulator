package main

import (
	"fmt"
	"sync"
	"time"
)

var reqChan chan *Request
var resChan chan *Request
var numOfReqs int
var clsSize int
var wg sync.WaitGroup

var maxOutStanding int
var fakeProcessTime time.Duration
var fakeRequestDelayTime time.Duration

func init() {
	reqChan = make(chan *Request)
	resChan = make(chan *Request)
	numOfReqs = 100
	clsSize = 5
	maxOutStanding = 10
	fakeRequestDelayTime = 0 * time.Second
	fakeProcessTime = 7 * time.Second
	// add wait groups
	wg.Add(numOfReqs)
}

func process(num int) (int, error) {
	// sleep, for simulating some time taking task
	time.Sleep(fakeProcessTime)
	return num * num, nil
}

// simulating client req
func requestEmitter(numOfReqs int) {
	for i := 1; i <= numOfReqs; i++ {
		time.Sleep(fakeRequestDelayTime) // request will be sent after every X seconds
		// fmt.Printf("sending- %d\n", i)
		reqChan <- &Request{num: i, msg: "client request", id: i}
	}
	close(reqChan)
}

// simulating capturing of response by client
func responseGrabber() {
	// var responseCollected int = 0
	for res := range resChan {
		fmt.Printf("RES: %+v\n", *res)
		// responseCollected++
		// fmt.Printf("response collected: %d\n", responseCollected)
		wg.Done() //mark the end of each req's response
	}
	close(resChan)
}

func main() {
	fmt.Printf("No of reqs:%d\nNo of servers: %d\nRate-Limit for each server: %d\n", numOfReqs, clsSize, maxOutStanding)

	// setup client simulation
	go requestEmitter(numOfReqs)
	go responseGrabber()

	// create a cluster
	// assuming request is coming from single channel
	// and all the server are allowed to send response to only one channel
	cls := CreateNewCluster(process, clsSize, maxOutStanding)
	cls.Start(reqChan, resChan)

	// kill some nodes
	go func() {
		time.Sleep(3 * time.Second)
		cls.KillNode(0)
		time.Sleep(3 * time.Second)
		cls.KillNode(2)
	}()
	start := time.Now()
	wg.Wait()
	fmt.Printf("Elapsed time: %0.5f\n", time.Since(start).Seconds())
}
