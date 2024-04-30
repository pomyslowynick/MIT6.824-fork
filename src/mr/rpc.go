package mr

import (
	"os"
	"strconv"
)

type ReduceNReduceIDRequest struct {
	ReducerID string
}

type ReduceNReduceIDReply struct {
	NReduceID int
	Finished  bool
}

type ReduceRequest struct {
	NReduceID int
	ReducerID string
}

type ReduceReply struct {
	Finished  bool
	Files     []string
	ReducerID int
}

type ReduceCompleteRequest struct {
	ReducerID string
}

type ReduceCompleteReply struct {
	Filename string
}

type CompleteRequest struct {
	WorkerID          string
	MapperOutputFiles map[int]string
}

type CompleteReply struct {
}

type TaskRequest struct {
	WorkerID string
}

type TaskReply struct {
	Finished bool
	Filename string
	NReduce  int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
