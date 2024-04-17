package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ReduceRequest struct {
	ReducerID string
}

type ReduceReply struct {
	Finished  bool
	Filename  string
	ReducerID int
}

type CompleteRequest struct {
	WorkerID          string
	MapperOutputFiles []string
}

type CompleteReply struct {
	Filename string
}

type TaskRequest struct {
	WorkerID string
}

type TaskReply struct {
	Finished bool
	Filename string
	NReduce  int
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
