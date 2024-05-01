package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type FileStream struct {
	Stream    chan string
	FileCount int
}

type Coordinator struct {
	// Your definitions here.
	FileList          []string
	FileListChan      chan string
	UnusedFiles       []string
	UsedFiles         []string
	Workers           map[string]WorkerEntry
	NReduce           int
	UnusedReduceFiles map[int]FileStream
	UsedReduceFiles   map[int][]string
	NReduceChan       chan int
	Reducers          map[string]WorkerEntry
	JobComplete       bool
}

type WorkerEntry struct {
	WorkerID string
	// TODO: Change to files
	AssignedFile string
	ReduceFiles  []string
	TimeStarted  time.Time
	State        string
	NReduce      int
}

func (c *Coordinator) RequestReduceComplete(args *ReduceCompleteRequest, reply *ReduceCompleteReply) error {
	if entry, ok := c.Reducers[args.ReducerID]; ok {
		entry.State = "completed"
	}
	// c.UsedReduceFiles = append(c.UsedReduceFiles, args.Filename)
	return nil
}

func (c *Coordinator) RequestComplete(args *CompleteRequest, reply *CompleteReply) error {
	if entry, ok := c.Workers[args.WorkerID]; ok {
		entry.State = "completed"
	}

	for key, file := range args.MapperOutputFiles {
		fileStream := c.UnusedReduceFiles[key]
		// fmt.Println("Writing to Stream: ", key, " chan. File: ", file, " channel: ", fileStream.Stream)
		fileStream.Stream <- file
		// fmt.Println("Wrote to Stream: ", key, " chan", " file: ", file)

		c.UnusedReduceFiles[key] = FileStream{c.UnusedReduceFiles[key].Stream, c.UnusedReduceFiles[key].FileCount + 1}
		// fmt.Println("Closing channel num: ", key, "file count is: ", fileStream.FileCount, " and file list len: ", len(c.FileList), " and the channel is: ", fileStream.Stream)
		if c.UnusedReduceFiles[key].FileCount == len(c.FileList) {
			close(c.UnusedReduceFiles[key].Stream)
		}
	}

	return nil
}

func (c *Coordinator) RequestTask(args *TaskRequest, reply *TaskReply) error {
	if assignedFile, ok := <-c.FileListChan; ok {
		if !ok {
			reply.Finished = true
			return nil
		}
		reply.Filename = assignedFile
		c.UsedFiles = append(c.UsedFiles, assignedFile)

		reply.NReduce = c.NReduce
		// TODO: Chck that we are not adding redundant entries to Workers
		w := WorkerEntry{WorkerID: args.WorkerID, AssignedFile: assignedFile, TimeStarted: time.Now(), State: "working"}

		// Not sure if we need to keep those in a global state var
		// Could instead fire off a goroutine per worker to check back?
		// How do we then know when it's request came back?
		c.Workers[args.WorkerID] = w

	} else {
		reply.Finished = true
	}
	// TODO: Run async task to check on result after 60 seconds

	// time.Sleep(1 * time.Second)
	return nil
}

func (c *Coordinator) RequestNReduceID(args *ReduceNReduceIDRequest, reply *ReduceNReduceIDReply) error {
	NReduce, ok := <-c.NReduceChan
	// fmt.Println("Got NReduce id")
	if !ok {
		fmt.Println("no more IDs, finishing up")
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
	w := WorkerEntry{WorkerID: args.ReducerID, ReduceFiles: reply.Files, TimeStarted: time.Now()}
	c.Reducers[args.ReducerID] = w
	// fmt.Println("Before the range over files stream")
	for file := range c.UnusedReduceFiles[NReduceID].Stream {
		reply.Files = append(reply.Files, file)
		// c.UsedReduceFiles[NReduceID] = append(c.UsedReduceFiles[NReduceID], c.UnusedReduceFiles.Files[NReduceID]...)
		// c.UnusedReduceFiles.Files[NReduceID] = nil

		// if len(c.UnusedReduceFiles) != 1 {
		// 	c.UnusedReduceFiles[NReduceID] = c.UnusedReduceFiles[NReduceID][1:]
		// }

	}
	// fmt.Println("Outside files stream")
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
	c.UnusedFiles = files
	c.NReduce = nReduce
	c.Workers = make(map[string]WorkerEntry)
	c.Reducers = make(map[string]WorkerEntry)
	c.UnusedReduceFiles = make(map[int]FileStream)
	c.UsedReduceFiles = make(map[int][]string)

	c.FileListChan = make(chan string)

	go func() {
		defer close(c.FileListChan)
		for _, file := range files {
			c.FileListChan <- file
		}
	}()

	for i := 0; i < nReduce; i++ {
		stream := FileStream{make(chan string, nReduce), 0}
		c.UnusedReduceFiles[i] = stream
	}

	c.NReduceChan = make(chan int)
	go func() {
		defer close(c.NReduceChan)
		for i := 0; i < nReduce; i++ {
			// fmt.Println("Writing to NReduce chan")
			c.NReduceChan <- i
			// fmt.Println("after writing to NReduce chan")
		}
	}()

	c.server()
	return &c
}
