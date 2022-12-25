package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync/atomic"


type Coordinator struct {
	// Your definitions here.
	files []string
	mappers chan int // mapper task id
	reducers chan int
	nMapper int
	nReduce int
	done chan int
	nMapperSuccs int32
	nReduceSuccs int32
	tMapperStatus []int32
	tReduceStatus []int32
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) monitorTask(taskType, taskId int) {
	time.Sleep(time.Second * 10)
	// check if taskId success
	if taskType == MAPPER {
		if atomic.LoadInt32(&c.tMapperStatus[taskId]) == 0 {
			// still not complete, rerun the task
			c.mappers <- taskId
		}
	}
	if taskType == REDUCER {
		if atomic.LoadInt32(&c.tReduceStatus[taskId]) == 0 {
			c.reducers <- taskId
		}
	}
}

func (c *Coordinator) GetTask(req *GetTaskRequest, res *GetTaskResponse) error {
	// use channel instead of mutex lock
	mapperTaskId, ok := <-c.mappers
	if ok {
		res.TaskType = MAPPER
		res.TaskId = mapperTaskId
		res.MapperFilename = c.files[mapperTaskId]
		res.NMapper = c.nMapper
		res.NReduce = c.nReduce
		res.Code = MAPPER_TASK
		go c.monitorTask(MAPPER, mapperTaskId)
		return nil
	}

	// bug! must wait all mapper complete
	if atomic.LoadInt32(&c.nMapperSuccs) < int32(c.nMapper) {
		res.Code = WAIT
		return nil
	}

	reduceTaskId, ok := <-c.reducers
	if ok {
		res.TaskType = REDUCER
		res.TaskId = reduceTaskId
		res.NMapper = c.nMapper
		res.NReduce = c.nReduce
		res.Code = REDUCE_TASK
		go c.monitorTask(REDUCER, reduceTaskId)
		return nil
	}

	// fix early exit, need wait all reduce tasks complete then workers can exit
	if atomic.LoadInt32(&c.nReduceSuccs) < int32(c.nReduce) {
		res.Code = WAIT
		return nil
	}

	res.Msg = "done"
	res.Code = DONE
	return nil
}

func (c *Coordinator) TaskSuccess(req *TaskSuccessRequest, res *TaskSuccessResponse) error {
	if req.TaskType == MAPPER {
		atomic.AddInt32(&c.nMapperSuccs, 1)
		atomic.AddInt32(&c.tMapperStatus[req.TaskId], 1)
	} else if req.TaskType == REDUCER {
		atomic.AddInt32(&c.nReduceSuccs, 1)
		atomic.AddInt32(&c.tReduceStatus[req.TaskId], 1)
	}
	if req.TaskType == MAPPER && atomic.LoadInt32(&c.nMapperSuccs) >= int32(c.nMapper) {
		close(c.mappers)
	}
	if req.TaskType == REDUCER && atomic.LoadInt32(&c.nReduceSuccs) >= int32(c.nReduce) {
		close(c.reducers)
		c.done <- 1
	}
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	select {
		case <-c.done:
			return true
		case <-time.After(time.Second * 0):
			return false
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.mappers = make(chan int, len(files))
	for i := 0; i < len(files); i++ {
		c.mappers <- i
	}
	// close(c.mappers)
	c.reducers = make(chan int, nReduce)
	for i := 0; i< nReduce; i++ {
		c.reducers <- i
	}
	// close(c.reducers)
	c.nMapper = len(files)
	c.nReduce = nReduce
	c.done = make(chan int)
	c.tMapperStatus = make([]int32, len(files))
	c.tReduceStatus = make([]int32, nReduce)


	c.server()
	return &c
}
