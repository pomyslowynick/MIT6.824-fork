package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	FileList          []string
	FileListChan      chan string
	CompletedFiles    chan int
	Workers           map[string]WorkerEntry
	NReduce           int
	UnusedReduceFiles map[int]chan string
	UsedReduceFiles   map[int][]string
	NReduceChan       chan int
	Reducers          map[string]WorkerEntry
	JobComplete       bool
	HealthCheckLock   sync.Mutex
	FileCountLock     sync.Mutex
	ReducersMapLock   sync.Mutex
}

type WorkerEntry struct {
	WorkerID             string
	AssignedFile         string
	ReduceFiles          []string
	HealthCheckTimestamp time.Time
	State                string
	NReduce              int
}

func (c *Coordinator) RequestHealthCheck(args *HealthCheckRequest, reply *HealthCheckReply) error {
	c.HealthCheckLock.Lock()
	tempObject := c.Workers[args.WorkerID]
	tempObject.HealthCheckTimestamp = time.Now()
	c.Workers[args.WorkerID] = tempObject
	c.HealthCheckLock.Unlock()
	return nil
}

func (c *Coordinator) RequestReduceComplete(args *ReduceCompleteRequest, reply *ReduceCompleteReply) error {
	if entry, ok := c.Reducers[args.ReducerID]; ok {
		entry.State = "completed"
	}
	return nil
}

func (c *Coordinator) RequestComplete(args *CompleteRequest, reply *CompleteReply) error {
	if entry, ok := c.Workers[args.WorkerID]; ok {
		entry.State = "completed"
	}

	for key, file := range args.MapperOutputFiles {
		fileStream := c.UnusedReduceFiles[key]
		fileStream <- file
	}

	c.CompletedFiles <- 1

	return nil
}

func (c *Coordinator) RequestTask(args *TaskRequest, reply *TaskReply) error {
	if assignedFile, ok := <-c.FileListChan; ok {
		if !ok {
			reply.Finished = true
			return nil
		}
		reply.Filename = assignedFile

		reply.NReduce = c.NReduce
		w := WorkerEntry{WorkerID: args.WorkerID, AssignedFile: assignedFile, HealthCheckTimestamp: time.Now(), State: "working"}

		c.Workers[args.WorkerID] = w

	} else {
		reply.Finished = true
	}
	return nil
}

func (c *Coordinator) RequestNReduceID(args *ReduceNReduceIDRequest, reply *ReduceNReduceIDReply) error {
	NReduce, ok := <-c.NReduceChan
	if !ok {
		reply.Finished = true
		c.JobComplete = true
		return nil
	}
	reply.NReduceID = NReduce
	retrievedReducer := c.Reducers[args.ReducerID]
	retrievedReducer.NReduce = NReduce
	return nil
}

func (c *Coordinator) RequestReduce(args *ReduceRequest, reply *ReduceReply) error {
	NReduceID := args.NReduceID
	w := WorkerEntry{WorkerID: args.ReducerID, HealthCheckTimestamp: time.Now()}

	c.ReducersMapLock.Lock()
	c.Reducers[args.ReducerID] = w
	c.ReducersMapLock.Unlock()
	for file := range c.UnusedReduceFiles[NReduceID] {
		reply.Files = append(reply.Files, file)
	}
	reply.Finished = true
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	if c.JobComplete {
		ret = true
		fmt.Println("MapReduce has completed")
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.FileList = files
	c.NReduce = nReduce
	c.Workers = make(map[string]WorkerEntry)
	c.Reducers = make(map[string]WorkerEntry)
	c.UnusedReduceFiles = make(map[int]chan string)
	c.UsedReduceFiles = make(map[int][]string)
	c.FileListChan = make(chan string)

	go func() {
		for _, file := range files {
			c.FileListChan <- file
		}
	}()

	c.NReduceChan = make(chan int, nReduce)

	for i := 0; i < nReduce; i++ {
		c.NReduceChan <- i
	}

	for i := 0; i < nReduce; i++ {
		c.UnusedReduceFiles[i] = make(chan string, nReduce)
	}

	go func() {
		for {
			time.Sleep(time.Second * 10)
			for _, worker := range c.Workers {
				if time.Since(worker.HealthCheckTimestamp) > time.Second*10 {
					fmt.Println("10 seconds passed since workers ", worker.WorkerID, " last successful healthcheck")
					c.FileListChan <- worker.AssignedFile
				}
			}
		}
	}()

	c.CompletedFiles = make(chan int)
	go func() {
		completedFilesCounter := 0
		for {
			count := <-c.CompletedFiles
			completedFilesCounter += count
			if completedFilesCounter == len(c.FileList) {
				close(c.CompletedFiles)
				close(c.NReduceChan)
				close(c.FileListChan)
				for _, stream := range c.UnusedReduceFiles {
					close(stream)
				}
				break
			}
		}
	}()
	c.server()
	return &c
}
