package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// Add your RPC definitions here.
const (
	NO_TASK = iota
	MAP_TASK
	REDUCE_TASK
	SHUTDOWN
)

const (
	FREE = iota
	MAP_DONE
	REDUCE_DONE
)

type TaskArgs struct {
	Status  int // 0->free 1->finish map task 2-> finish reduce task
	Paths   []string
	TaskNum int
}

type TaskReply struct {
	TaskType int // 0-> no task, 1->map, 2->reduce, 3->shutdown
	Paths    []string
	TaskNum  int
	NWorker  int
	NReduce  int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
