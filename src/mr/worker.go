package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"strings"

	"github.com/google/uuid"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	workerID := uuid.NewString()

	// Request a Task, refactor later into a method
	args := TaskRequest{WorkerID: workerID}
	reply := TaskReply{}

	ok := call("Coordinator.RequestTask", &args, &reply)
	if ok {
		fmt.Printf("reply %v\n", reply.Filename)
	} else {
		fmt.Printf("call failed!\n")
	}

	openedFile, err := os.Open(reply.Filename)
	if err != nil {
		fmt.Println(err)
	}

	// wordsMap := make(map[string]int)
	scanner := bufio.NewScanner(openedFile)
	var fulltext []string
	for scanner.Scan() {
		fulltext = append(fulltext, scanner.Text())
	}
	// for i := 0; scanner.Scan(); i++ {
	// 	splitLine := strings.Split(scanner.Text(), " ")
	// 	for _, word := range splitLine {
	// 		if len(word) == 0 {
	// 			break
	// 		}
	// 		wordsMap[word]++
	// 	}
	// }
	// fmt.Println(wordsMap)
	result := mapf(reply.Filename, strings.Join(fulltext, " "))

	// argsComplete := CompleteRequest{Result: wordsMap, WorkerID: workerID}
	argsComplete := CompleteRequest{Result: result, WorkerID: workerID}
	replyComplete := CompleteReply{}
	ok = call("Coordinator.RequestComplete", &argsComplete, &replyComplete)

	// argsComplete := CompleteRequest{Result: wordsMap, WorkerID: workerID}
	argsComplete := ReduceRequest{Result: result, WorkerID: workerID}
	replyComplete := ReduceReply{}
	ok = call("Coordinator.RequestComplete", &argsComplete, &replyComplete)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
