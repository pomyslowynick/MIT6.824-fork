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
	FileList    []string
	UnusedFiles []string
	UsedFiles   []string
	Workers     []WorkerEntry
	NReduce     int
}

type WorkerEntry struct {
	WorkerID     string
	AssignedFile string
	TimeStarted  time.Time
}

func (c *Coordinator) RequestComplete(args *CompleteRequest, reply *CompleteReply) error {
	fmt.Println(args.Result)

	return nil
}

func (c *Coordinator) RequestTask(args *TaskRequest, reply *TaskReply) error {
	reply.Filename = c.UnusedFiles[0]
	c.UnusedFiles = c.UnusedFiles[1:]
	c.UsedFiles = append(c.UsedFiles, c.UnusedFiles[0])
	w := WorkerEntry{WorkerID: args.WorkerID, AssignedFile: reply.Filename, TimeStarted: time.Now()}

	// Not sure if we need to keep those in a global state var
	// Could instead fire off a goroutine per worker to check back?
	// How do we then know when it's request came back?
	c.Workers = append(c.Workers, w)
	fmt.Println(c.UsedFiles)

	// TODO: Run async task to check on result after 60 seconds

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
	fmt.Println(files)
	// Your code here.

	c.server()
	return &c
}
