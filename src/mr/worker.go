package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

var workerID string

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

	workerID = uuid.NewString()

	go healthProbe(workerID)
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
				// fmt.Println("[Worker: ", workerID, "]", "Mapped files are: ", mappedFiles)
				replyComplete := CompleteReply{}
				// time.Sleep(10 * time.Millisecond)
				ok = call("Coordinator.RequestComplete", &argsComplete, &replyComplete)
			}
		} else {
			fmt.Printf("call failed! Retrying: \n")
		}
	}

	reducerID := uuid.NewString()
	// fmt.Println("[Worker: ", workerID, "] ", "Finished mapping, proceeding to reducing")
	// argsComplete := CompleteRequest{Result: wordsMap, WorkerID: workerID}
	// reducerID := reply.ReducerID
	for {
		argsReduceID := ReduceNReduceIDRequest{ReducerID: reducerID}
		replyReduceID := ReduceNReduceIDReply{}
		ok := call("Coordinator.RequestNReduceID", &argsReduceID, &replyReduceID)
		// fmt.Println("[Worker: ", workerID, "] ", "I got a nreduceID: ", replyReduceID.NReduceID)
		if replyReduceID.Finished {
			break
		}

		if ok {
			for {
				// fmt.Println("Making a reduce request")
				argsReduce := ReduceRequest{ReducerID: reducerID, NReduceID: replyReduceID.NReduceID}
				replyReduce := ReduceReply{}
				ok := call("Coordinator.RequestReduce", &argsReduce, &replyReduce)
				if ok {
					// ProcessTask(reply.Filename, mapf)
					// argsComplete := CompleteRequest{Result: wordsMap, WorkerID: workerID}
					if err := ProcessReduceTask(replyReduce.Files, reducef, replyReduceID.NReduceID); err == nil {
						argsComplete := ReduceCompleteRequest{ReducerID: reducerID}
						replyComplete := ReduceCompleteReply{}
						// time.Sleep(20 * time.Millisecond)
						// fmt.Println("Calling reduce complete")
						ok = call("Coordinator.RequestReduceComplete", &argsComplete, &replyComplete)
					} else {
						panic(fmt.Sprintf("failed to run reduce on files: %v", replyReduce.Files))

					}
					if replyReduce.Finished {
						break
					}
				} else {
					fmt.Printf("call failed! Retrying: \n")
				}
			}
		} else {
			fmt.Printf("couldn't obtain reduceID: \n")
		}
	}
}

func ProcessReduceTask(files []string, reducef func(string, []string) string, nReduceID int) error {
	var totalKeys []KeyValue

	fmt.Println("[Worker: ", workerID, "] ", "Files received in reduce are:", files)
	for _, filename := range files {
		openedFile, err := os.Open(filename)
		if err != nil {
			fmt.Println("failed in ProcessReduceTask reading file:", err)
			return err
		}

		var decodedKeys []KeyValue
		enc := json.NewDecoder(openedFile)

		err = enc.Decode(&decodedKeys)
		if err != nil {
			fmt.Println("error decoding file: ", err)
			return err
		}

		totalKeys = append(totalKeys, decodedKeys...)
	}

	sort.Sort(ByKey(totalKeys))

	outputFile, err := os.Open(fmt.Sprintf("mr-out-%v", nReduceID))
	if err != nil {
		outputFile, err = os.Create(fmt.Sprintf("mr-out-%v", nReduceID))
		if err != nil {
			return err
		}
	}

	for i := 0; i < len(totalKeys); {
		j := i + 1

		for j < len(totalKeys) && totalKeys[i].Key == totalKeys[j].Key {
			j++
		}

		values := []string{}

		for k := i; k < j; k++ {
			values = append(values, totalKeys[k].Value)
		}

		result := reducef(totalKeys[i].Key, values)

		outputFile.WriteString(fmt.Sprintf("%v %v\n", totalKeys[i].Key, result))
		i = j
	}
	return nil
}

func ProcessTask(filename string, mapf func(string, string) []KeyValue, NReduce int, workerID string) (map[int]string, error) {
	openedFile, err := os.Open(filename)
	if err != nil {
		fmt.Println("failed in ProcessTask reading file:", err)
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

	mappedFiles := make(map[int]string)

	for key, _ := range intermediate {
		mappedFilename := fmt.Sprintf("%v-%v", filename, key)
		file, err := os.Create(mappedFilename)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
		mappedFiles[key] = mappedFilename

		sort.Sort(ByKey(intermediate[key]))

		enc := json.NewEncoder(file)
		enc.Encode(intermediate[key])
	}

	return mappedFiles, nil
}

func healthProbe(workerID string) {
	args := HealthCheckRequest{WorkerID: workerID}
	reply := HealthCheckReply{}
	for {
		time.Sleep(time.Second * 2)
		call("Coordinator.RequestHealthCheck", &args, &reply)
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
