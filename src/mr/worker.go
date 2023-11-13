package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// 任务定义
type Task struct {
	TaskType   TaskType //任务类型
	TaskId     int      //任务id
	ReducerNum int      //reducer数量
	Filename   []string //输入文件的切片，map阶段一一对应，而reduce阶段则是对应多个tmp中间文件
}

// 任务类型的定义
type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaittingTask //表示此时通道内的任务都分发完了，所以这个任务需要等待通道内有新的任务可以获取
	ExitTask     //当项目阶段达到alldone时，任务就会得到exittask阶段
)

// 阶段的定义,针对coordinator使用
type ProjectPhase int

const (
	MapPhase ProjectPhase = iota
	ReducePhase
	Alldone
)

// 任务状态的定义
type TaskState int

const (
	Working TaskState = iota
	Waiting
	Done
)

//阶段应该是对这个项目整体而言，任务则针对具体的每一个worker，而任务状态则是每个任务内部的完成进度

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// 用于call的时候传入参数，没有实际用处，因此不需要定义
type TaskArgs struct{}

// 用于后续reduce使用时对中间结果进行排序
type SortedKey []KeyValue

// Len 重写len,swap,less才能排序
func (k SortedKey) Len() int           { return len(k) }
func (k SortedKey) Swap(i, j int)      { k[i], k[j] = k[j], k[i] }
func (k SortedKey) Less(i, j int) bool { return k[i].Key < k[j].Key }

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

	// Your worker implementation here.
	// 启动一个for循环不断轮询从coordinator请求工作,当前状态为
	for {
		// worker从coordinator获得任务
		task := getTask()
		switch task.TaskType {
		case MapTask:
			{
				DoMapWork(&task, mapf)
				taskFinished(&task)
			}
		case ReduceTask:
			{
				DoReduceWork(&task, reducef)
				taskFinished(&task)
			}
		case WaittingTask:
			{
				//fmt.Println("All tasks are in progress. Please wait for a while")
				time.Sleep(time.Second)
			}
		case ExitTask:
			{
				//fmt.Println("Task", task.TaskId, "is terminated")
				break
			}
		}

	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func getTask() Task {
	args := TaskArgs{}
	reply := Task{}

	msg := call("Coordinator.AssignTask", &args, &reply)

	if msg {
		//fmt.Println(reply)
	} else {
		//fmt.Println("call failed!")
	}
	return reply
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

func DoMapWork(task *Task, mapf func(string, string) []KeyValue) {
	filename := task.Filename[0]

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("can't open %v", filename)
	}

	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("can't read %v", filename)
	}
	file.Close()

	//map返回一组KV结构体数组
	intermediate := mapf(filename, string(content))

	// 以下代码实现的是中间数据的存储
	rn := task.ReducerNum

	// 创建一个长度为reducernum的二维切片
	HashedKV := make([][]KeyValue, rn)

	for _, kv := range intermediate {
		HashedKV[ihash(kv.Key)%rn] = append(HashedKV[ihash(kv.Key)%rn], kv)
	}

	//创建中间文件保存数据
	//这里感觉有一点问题，应该用临时文件暂时保存一下防止crash挂掉，回头这里代码要改的
	for i := 0; i < rn; i++ {
		tmpname := "mr-tmp-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		tmpfile, _ := os.Create(tmpname)
		enc := json.NewEncoder(tmpfile)

		for _, kv := range HashedKV[i] {
			enc.Encode(kv)
		}
		tmpfile.Close()
	}
}

func DoReduceWork(task *Task, reducef func(string, []string) string) {
	reduceFileIdx := task.TaskId
	intermediate := shuffle(task.Filename)
	dir, _ := os.Getwd()

	tempFile, err := os.CreateTemp(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}

	// 这部分代码逻辑照抄给的example
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempFile.Close()
	fn := fmt.Sprintf("mr-out-%d", reduceFileIdx)
	os.Rename(tempFile.Name(), fn)

}

// 将kv数组排序
func shuffle(files []string) []KeyValue {
	var kva []KeyValue
	for _, filepath := range files {
		file, _ := os.Open(filepath)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	//按键值进行排序
	sort.Sort(SortedKey(kva))
	return kva
}

func taskFinished(task *Task) Task {
	reply := Task{}

	ok := call("Coordinator.TaskFinished", task, &reply)

	// 按理说这段代码是没必要存在的
	if ok {
		//fmt.Println(reply)
	} else {
		//fmt.Printf("call failed!\n")
	}

	return reply
}
