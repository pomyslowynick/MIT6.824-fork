package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"strings"
	"time"

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

	for {
		ok := call("Coordinator.RequestTask", &args, &reply)
		if ok {
			if reply.Finished {
				break
			}
			if mappedFiles, err := ProcessTask(reply.Filename, mapf, reply.NReduce, workerID); err == nil {
				// argsComplete := CompleteRequest{Result: wordsMap, WorkerID: workerID}
				argsComplete := CompleteRequest{WorkerID: workerID, MapperOutputFiles: mappedFiles}
				replyComplete := CompleteReply{}
				time.Sleep(100 * time.Millisecond)
				ok = call("Coordinator.RequestComplete", &argsComplete, &replyComplete)
			}
		} else {
			fmt.Printf("call failed! Retrying: \n")
		}
	}

	reducerID := uuid.NewString()
	// argsComplete := CompleteRequest{Result: wordsMap, WorkerID: workerID}
	argsReduce := ReduceRequest{ReducerID: reducerID}
	replyReduce := ReduceReply{}
	ok := call("Coordinator.RequestReduce", &argsReduce, &replyReduce)
	// reducerID := reply.ReducerID
	for {
		if ok {
			if replyReduce.Finished {
				break
			}
			// ProcessTask(reply.Filename, mapf)
			// argsComplete := CompleteRequest{Result: wordsMap, WorkerID: workerID}
			if err := ProcessReduceTask(replyReduce.Filename, reducef); err == nil {
				argsComplete := CompleteRequest{WorkerID: workerID}
				replyComplete := CompleteReply{}
				time.Sleep(500 * time.Millisecond)
				ok = call("Coordinator.RequestComplete", &argsComplete, &replyComplete)
			}
		} else {
			fmt.Printf("call failed! Retrying: \n")
		}
	}
}

func ProcessReduceTask(filename string, reducef func(string, []string) string) error {
	openedFile, err := os.Open(filename)
	if err != nil {
		fmt.Println(err)
		return err
	}

	enc := json.NewDecoder(openedFile)

	var decodedKeys []KeyValue
	err = enc.Decode(decodedKeys)
	if err != nil {
		fmt.Println(err)
		return err
	}

	fmt.Println(decodedKeys)
	// result := reducef(filename, strings.Join(fulltext, " "))
	// intermediate := make(map[int][]KeyValue)

	// for _, kv := range result {
	// 	intermediate[ihash(kv.Key)%NReduce] = append(intermediate[ihash(kv.Key)%NReduce], kv)
	// }

	// fmt.Println(intermediate)

	// var mappedFiles []string

	// for key, _ := range intermediate {
	// 	mappedFilename := fmt.Sprintf("%v-%v-%v", workerID, filename, key)
	// 	file, err := os.Create(mappedFilename)
	// 	if err != nil {
	// 		fmt.Println(err)
	// 		return err
	// 	}
	// 	mappedFiles = append(mappedFiles, mappedFilename)

	// 	enc.Encode(intermediate[key])
	// }

	return nil
}

func ProcessTask(filename string, mapf func(string, string) []KeyValue, NReduce int, workerID string) ([]string, error) {
	openedFile, err := os.Open(filename)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	// wordsMap := make(map[string]int)
	scanner := bufio.NewScanner(openedFile)
	var fulltext []string
	for scanner.Scan() {
		fulltext = append(fulltext, scanner.Text())
	}
	result := mapf(filename, strings.Join(fulltext, " "))
	intermediate := make(map[int][]KeyValue)

	for _, kv := range result {
		intermediate[ihash(kv.Key)%NReduce] = append(intermediate[ihash(kv.Key)%NReduce], kv)
	}

	fmt.Println(intermediate)

	var mappedFiles []string

	for key, _ := range intermediate {
		mappedFilename := fmt.Sprintf("%v-%v-%v", workerID, filename, key)
		file, err := os.Create(mappedFilename)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
		mappedFiles = append(mappedFiles, mappedFilename)

		enc := json.NewEncoder(file)
		enc.Encode(intermediate[key])
	}

	return mappedFiles, nil
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
