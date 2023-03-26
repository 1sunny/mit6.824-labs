package mr

import (
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	nReduce       int
	nWorker       int
	files         []string
	nMapDone      int
	nReduceDone   int
	mapStatus     []int
	reduceStatus  []int
	interFilePath [][]string
	mutex         sync.Mutex
}

const (
	UNASSIGNED = iota
	ASSIGNED
	DONE
)

func (m *Master) getMapTask() int {
	// try unassigned map task
	for i := 0; i < m.nWorker; i++ {
		if m.mapStatus[i] == UNASSIGNED {
			return i
		}
	}
	// try assigned map task
	for i := 0; i < m.nWorker; i++ {
		if m.mapStatus[i] == ASSIGNED {
			return i
		}
	}
	return -1
}

func (m *Master) getReduceTask() int {
	if m.nMapDone != m.nWorker {
		return -1
	}
	// try unassigned reduce task
	for i := 0; i < m.nReduce; i++ {
		if m.reduceStatus[i] == UNASSIGNED {
			return i
		}
	}
	// try assigned reduce task
	for i := 0; i < m.nReduce; i++ {
		if m.reduceStatus[i] == ASSIGNED {
			return i
		}
	}
	return -1
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) AssignTask(args *TaskArgs, reply *TaskReply) error {
	log.Println("Receive args: ", args)
	m.mutex.Lock()
	defer m.mutex.Unlock()
	switch args.Status {
	case FREE:
	case MAP_DONE:
		if m.mapStatus[args.TaskNum] != DONE {
			m.mapStatus[args.TaskNum] = DONE
			for i := 0; i < m.nReduce; i++ {
				m.interFilePath[args.TaskNum][i] = args.Paths[i]
			}
			m.nMapDone += 1
		}
	case REDUCE_DONE:
		if m.reduceStatus[args.TaskNum] != DONE {
			m.reduceStatus[args.TaskNum] = DONE
			m.nReduceDone += 1
		}
	default:
		log.Fatal("Status Error")
	}
	reply.NWorker = m.nWorker
	reply.NReduce = m.nReduce
	taskIdx := m.getMapTask()
	if taskIdx != -1 {
		reply.TaskType = MAP_TASK
		reply.TaskNum = taskIdx
		reply.Paths = []string{m.files[taskIdx]}
		return nil
	}
	taskIdx = m.getReduceTask()
	if taskIdx != -1 {
		reply.TaskType = REDUCE_TASK
		reply.TaskNum = taskIdx
		reply.Paths = []string{}
		for i := 0; i < m.nWorker; i++ {
			reply.Paths = append(reply.Paths, m.interFilePath[i][taskIdx])
		}
		return nil
	}
	if m.nMapDone == m.nWorker && m.nReduce == m.nReduceDone {
		reply.TaskType = SHUTDOWN
	} else {
		reply.TaskType = NO_TASK
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Your code here.
	return m.nReduce == m.nReduceDone && m.nWorker == m.nMapDone
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.nReduce = nReduce
	m.nWorker = len(files)
	m.files = files
	m.mapStatus = make([]int, m.nWorker)
	m.reduceStatus = make([]int, m.nReduce)
	m.interFilePath = make([][]string, m.nWorker)
	for i := range m.interFilePath {
		m.interFilePath[i] = make([]string, nReduce)
	}

	m.server()
	return &m
}
