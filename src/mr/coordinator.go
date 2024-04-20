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
	UnusedReduceFiles map[int][]string
	UsedReduceFiles   map[int][]string
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
		c.UnusedReduceFiles[key] = append(c.UnusedReduceFiles[key], file)
	}
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

	return nil
}

func (c *Coordinator) RequestNReduceID(args *ReduceNReduceIDRequest, reply *ReduceNReduceIDReply) error {
	if c.NReduce < 0 {
		reply.Finished = true
		c.JobComplete = true
		return nil
	}
	reply.NReduceID = c.NReduce
	retrievedReducer := c.Reducers[args.ReducerID]
	retrievedReducer.NReduce = c.NReduce
	c.NReduce--
	return nil
}

func (c *Coordinator) RequestReduce(args *ReduceRequest, reply *ReduceReply) error {
	// How to create a sticky ID for reducers where they process single file number, all of one from 0-9
	NReduceID := args.NReduceID
	if len(c.UnusedReduceFiles[NReduceID]) > 0 {
		reply.Files = c.UnusedReduceFiles[NReduceID]
		c.UsedReduceFiles[NReduceID] = append(c.UsedReduceFiles[NReduceID], c.UnusedReduceFiles[NReduceID]...)
		c.UnusedReduceFiles[NReduceID] = nil

		// if len(c.UnusedReduceFiles) != 1 {
		// 	c.UnusedReduceFiles[NReduceID] = c.UnusedReduceFiles[NReduceID][1:]
		// }

		w := WorkerEntry{WorkerID: args.ReducerID, ReduceFiles: reply.Files, TimeStarted: time.Now()}

		// Not sure if we need to keep those in a global state var
		// Could instead fire off a goroutine per worker to check back?
		// How do we then know when it's request came back?
		c.Reducers[args.ReducerID] = w
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

	if c.JobComplete {
		ret = true
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
	c.UnusedReduceFiles = make(map[int][]string)
	c.UsedReduceFiles = make(map[int][]string)
	// Your code here.

	c.server()
	return &c
}
