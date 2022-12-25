package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "time"
import "io/ioutil"
import "sort"
import "encoding/json"

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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	// request a map or reduce task from coordinator

	for {
		task := GetTaskRequest{}
		task.Time = time.Now().Unix()

		response := GetTaskResponse{}
		ok := call("Coordinator.GetTask", &task, &response)
		if !ok {
			log.Fatal("request task from coordinator failed: ", response.Msg)
			return
		}

		if response.Code == DONE && response.Msg == "done" {
			break
		}

		if response.Code == WAIT {
			time.Sleep(time.Millisecond * 10)
			continue
		}

		// fmt.Println("get new task:", response.TaskType, response.TaskId)

		if response.TaskType == MAPPER {
			file, err := os.Open(response.MapperFilename)
			if err != nil {
				log.Fatalf("cannot open %v", response.MapperFilename)
				return
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", response.MapperFilename)
				return
			}
			file.Close()

			ofiles := make([]*os.File, response.NReduce)
			for i := 0; i < len(ofiles); i++ {
				ofiles[i], _ = os.Create(fmt.Sprintf("mr-%d-%d", response.TaskId, i))
			}
			encs := make([]*json.Encoder, response.NReduce)
			for i := 0; i < len(encs); i++ {
				encs[i] = json.NewEncoder(ofiles[i])
			}

			kva := mapf(response.MapperFilename, string(content))

			for i := 0; i < len(kva); i++ {
				hash := ihash(kva[i].Key) % response.NReduce
				// fmt.Fprintf(ofiles[hash], "%v\n%v\n", kva[i].Key, kva[i].Value)
				err := encs[hash].Encode(&kva[i])
				if err != nil {
					log.Fatal("encoding json failed: ", err)
					// delete mr file and make coordinator to use another worker?
					break
				}
			}

			for i := 0; i < len(ofiles); i++ {
				ofiles[i].Close()
			}

			// response success
			pong := TaskSuccessRequest{}
			pong.TaskType = MAPPER
			pong.TaskId = response.TaskId
			call("Coordinator.TaskSuccess", &pong, &TaskSuccessResponse{})

		} else if response.TaskType == REDUCER {
			intermediate := []KeyValue{}
			for i := 0; i < response.NMapper; i++ {
				filename := fmt.Sprintf("mr-%d-%d", i, response.TaskId)
				// fmt.Println("reduce task:", response.TaskId, filename)
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}

				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}

				/*
				scanner := bufio.NewScanner(file)
				scanner.Split(bufio.ScanLines)
				for scanner.Scan() {
					k := scanner.Text()
					scanner.Scan()
					v := scanner.Text()
					intermediate = append(intermediate, KeyValue{k, v})
				}
				*/

				file.Close()
			}

			sort.Sort(ByKey(intermediate))

			oname := fmt.Sprintf("mr-out-%d", response.TaskId)
			ofile, _ := os.Create(oname)
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			ofile.Close()

			// response success
			pong := TaskSuccessRequest{}
			pong.TaskType = REDUCER
			pong.TaskId = response.TaskId
			ok := call("Coordinator.TaskSuccess", &pong, &TaskSuccessResponse{})

			if ok {
				// delete map intermediate file
				for i := 0; i < response.NMapper; i++ {
					filename := fmt.Sprintf("mr-%d-%d", i, response.TaskId)
					os.Remove(filename)
				}
			}

		} else {
			log.Fatal("response task type unsupport!")
		}
	}

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

	fmt.Println("rpc call failed:", err)
	return false
}
