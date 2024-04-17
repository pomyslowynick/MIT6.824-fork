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

type Coordinator struct {
	// Your definitions here.
	FileList          []string
	UnusedFiles       []string
	UsedFiles         []string
	Workers           map[string]WorkerEntry
	NReduce           int
	UnusedReduceFiles []string
	UsedReduceFiles   []string
}

type WorkerEntry struct {
	WorkerID     string
	AssignedFile string
	TimeStarted  time.Time
	State        string
}

func (c *Coordinator) RequestComplete(args *CompleteRequest, reply *CompleteReply) error {
	// fmt.Println(args.Result)
	if entry, ok := c.Workers[args.WorkerID]; ok {
		entry.State = "completed"
	}
	c.UnusedReduceFiles = append(c.UnusedReduceFiles, args.MapperOutputFiles...)
	// fmt.Println(c.UnusedReduceFiles)
	return nil
}

func (c *Coordinator) RequestTask(args *TaskRequest, reply *TaskReply) error {
	if len(c.UnusedFiles) > 0 {
		assignedFile := c.UnusedFiles[0]
		reply.Filename = assignedFile
		c.UsedFiles = append(c.UsedFiles, assignedFile)

		if len(c.UnusedFiles) == 1 {
			c.UnusedFiles = nil
		} else {
			c.UnusedFiles = c.UnusedFiles[1:]
		}

		reply.NReduce = c.NReduce
		w := WorkerEntry{WorkerID: args.WorkerID, AssignedFile: assignedFile, TimeStarted: time.Now(), State: "working"}

		// Not sure if we need to keep those in a global state var
		// Could instead fire off a goroutine per worker to check back?
		// How do we then know when it's request came back?
		c.Workers[args.WorkerID] = w
		// fmt.Println(c.UsedFiles)

	} else {
		reply.Finished = true
	}
	// TODO: Run async task to check on result after 60 seconds

	return nil
}

func (c *Coordinator) RequestReduce(args *ReduceRequest, reply *ReduceReply) error {
	if len(c.UnusedReduceFiles) > 0 {
		reply.Filename = c.UnusedReduceFiles[0]
		c.UsedReduceFiles = append(c.UsedReduceFiles, c.UnusedReduceFiles[0])

		if len(c.UnusedReduceFiles) != 1 {
			c.UnusedReduceFiles = c.UnusedReduceFiles[1:]
		}

		w := WorkerEntry{WorkerID: args.ReducerID, AssignedFile: reply.Filename, TimeStarted: time.Now()}

		// Not sure if we need to keep those in a global state var
		// Could instead fire off a goroutine per worker to check back?
		// How do we then know when it's request came back?
		c.Workers[args.ReducerID] = w
		// fmt.Println(c.UsedFiles)
		fmt.Println("Requesting reduce")

	} else {
		// TODO: Run async task to check on result after 60 seconds
		reply.Finished = true
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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

	// Your code here.

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
	// Your code here.

	c.server()
	return &c
}
