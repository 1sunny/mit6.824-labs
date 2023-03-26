package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	reply := getTask(FREE, []string{}, -1)
	for reply.TaskType != SHUTDOWN {
		nWorker := reply.NWorker
		nReduce := reply.NReduce
		taskNum := reply.TaskNum
		filepaths := reply.Paths
		log.Printf("Receive Task: TaskNum: %d, filepaths: %v", taskNum, filepaths)
		switch reply.TaskType {
		case NO_TASK:
			time.Sleep(time.Second)
			reply = getTask(FREE, []string{}, -1)
		case MAP_TASK:
			file, err := os.Open(filepaths[0])
			if err != nil {
				log.Fatalf("cannot open %v", filepaths[0])
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filepaths[0])
			}
			file.Close()

			kva := mapf(filepaths[0], string(content))
			intermediate := []KeyValue{}
			intermediate = append(intermediate, kva...)
			// sort.Sort(ByKey(intermediate)) ???

			// create temp files and encodes
			encs := []*json.Encoder{}
			tempFiles := []*os.File{}
			for i := 0; i < nReduce; i++ {
				tempFile, err := ioutil.TempFile(".", fmt.Sprintf("mr-%d-%d.temp", taskNum, i))
				if err != nil {
					log.Fatal("TempFile Error: ", err)
				}
				encs = append(encs, json.NewEncoder(tempFile))
				tempFiles = append(tempFiles, tempFile)
			}
			// encode k/v to temp files
			for _, kv := range intermediate {
				index := ihash(kv.Key) % nReduce
				err := encs[index].Encode(&kv)
				if err != nil {
					log.Fatal("Encode Error: ", err)
				}
			}
			interFilePath := []string{}
			// rename files
			for i := 0; i < nReduce; i++ {
				truePath := fmt.Sprintf("mr-%d-%d", taskNum, i)
				os.Rename(tempFiles[i].Name(), truePath)
				interFilePath = append(interFilePath, truePath)
			}
			// notify master job finished
			reply = getTask(MAP_DONE, interFilePath, taskNum)
		case REDUCE_TASK:
			kvs := []KeyValue{}
			for i := 0; i < nWorker; i++ {
				file, err := os.Open(filepaths[i])
				dec := json.NewDecoder(file)
				if err != nil {
					log.Fatal("Open Error: ", err)
				}
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kvs = append(kvs, kv)
				}
				file.Close()
			}
			sort.Sort(ByKey(kvs))

			tempFile, err := ioutil.TempFile(".", fmt.Sprintf("mr-out-%d.temp", taskNum))
			if err != nil {
				log.Fatal("TempFile Error: ", err)
			}
			i := 0
			for i < len(kvs) {
				j := i + 1
				for j < len(kvs) && kvs[j].Key == kvs[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kvs[k].Value)
				}
				output := reducef(kvs[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(tempFile, "%v %v\n", kvs[i].Key, output)

				i = j
			}
			os.Rename(tempFile.Name(), fmt.Sprintf("mr-out-%d", taskNum))
			reply = getTask(REDUCE_DONE, nil, taskNum)
		default:
			log.Fatal("Error reply.TaskType", reply.TaskType)
		}
	}
}

func getTask(status int, paths []string, taskNum int) *TaskReply {
	args := TaskArgs{status, paths, taskNum}
	reply := new(TaskReply)
	call("Master.AssignTask", &args, &reply)
	fmt.Printf("Reveive Reply: %v\n", reply)
	return reply
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
