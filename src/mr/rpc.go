package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

const (
	MAPPER = 1
	REDUCER = 2
	SUCCESS = 0
	MAPPER_TASK = 1
	REDUCE_TASK = 2
	WAIT = 3
	DONE = 4
)


type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type GetTaskRequest struct {
	Time int64
}

type GetTaskResponse struct {
	TaskType int
	TaskId int
	MapperFilename string
	NMapper int
	NReduce int
	Code int
	Msg string
}

type TaskSuccessRequest struct {
	TaskType int
	TaskId int
}

type TaskSuccessResponse struct {
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
