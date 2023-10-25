package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var mu sync.Mutex //全局锁，对Coordinator进行修改时的保障

type Coordinator struct {
	// Your definitions here.
	ReducerNum        int                   // reducer的数量
	TaskId            int                   // 任务的id
	Phase             ProjectPhase          // 目前项目处于什么阶段
	files             []string              // 保存存入的多个文件信息
	TaskMeta          map[int]*TaskMetaInfo // 保存task元数据
	TaskChannelMap    chan *Task            // 获取Map任务的通道
	TaskChannelReduce chan *Task            // 获取Reduce任务的通道
}

type TaskMetaInfo struct {
	Taskstate TaskState // 任务状态
	TaskPoint *Task     // 指向任务的指针
	StartTime time.Time // 任务的开始时间，为crash作准备
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()

	ret := false
	if c.Phase == Alldone {
		//fmt.Println("All tasks are finished, the coordinator will be exit!")
		ret = true
	}
	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:             files,
		ReducerNum:        nReduce,
		Phase:             MapPhase,
		TaskChannelMap:    make(chan *Task, len(files)),                    //这里貌似每个文件对应一个channel容量，论文中是按照文件大小
		TaskChannelReduce: make(chan *Task, nReduce),                       // 其实可以用一个通道代替两个，通道容量为max(nerduce len(files))
		TaskMeta:          make(map[int]*TaskMetaInfo, len(files)+nReduce), //任务的总数
	}
	// 创建Map任务
	c.createMapTasks()

	// Your code here.
	// 检查任务进行时间，将超过10s的任务都方会通道中，等待下次重新读取
	go c.CrashHandler()
	//启动rpc服务器
	c.server()
	return &c
}

func (c *Coordinator) createMapTasks() {
	for _, v := range c.files {
		//这里有两种方法可以获得id，一种就是用Coordinator自带的TaskId，或者用for循环自带的循环次数作为id也可以，这样就不要在Coordinator定义TaskId了
		idx := c.taskiditer()
		task := Task{
			TaskType:   MapTask,
			TaskId:     idx,
			ReducerNum: c.ReducerNum,
			Filename:   []string{v},
		}

		// 保存任务的初始状态
		taskmeta := TaskMetaInfo{
			Taskstate: Waiting,
			TaskPoint: &task,
		}

		//将task信息存入Coordinator结构体,这里其实可以不用做判别吧
		meta := c.TaskMeta[idx]
		if meta != nil {
			//fmt.Println("meta contains task which id = ", idx)
		} else {
			c.TaskMeta[idx] = &taskmeta
		}
		//fmt.Println("make a map task :", &task)

		c.TaskChannelMap <- &task
	}
}

func (c *Coordinator) createReduceTasks() {
	for i := 0; i < c.ReducerNum; i++ {
		idx := c.taskiditer()
		//fmt.Printf("generate reduce %d\n", idx)
		task := Task{
			TaskId:   idx,
			TaskType: ReduceTask,
			Filename: selectReduceName(i),
		}

		// 保存任务的初始状态
		taskmeta := TaskMetaInfo{
			Taskstate: Waiting,
			TaskPoint: &task,
		}

		//将task信息存入Coordinator结构体,这里其实可以不用做判别吧
		meta := c.TaskMeta[idx]
		if meta != nil {
			//fmt.Println("meta contains task which id = ", idx)
			return
		} else {
			c.TaskMeta[idx] = &taskmeta
		}
		//fmt.Println("make a reduce task :", &task)

		c.TaskChannelReduce <- &task
	}
}

func selectReduceName(reduceNum int) []string {
	var s []string
	path, _ := os.Getwd()
	files, _ := os.ReadDir(path)
	//fmt.Println(files)
	for _, fi := range files {
		// 匹配对应的reduce文件
		if strings.HasPrefix(fi.Name(), "mr-tmp") && strings.HasSuffix(fi.Name(), strconv.Itoa(reduceNum)) {
			s = append(s, fi.Name())
		}
	}
	//fmt.Println(s)
	return s
}

// 迭代任务id
func (c *Coordinator) taskiditer() int {
	msg := c.TaskId
	c.TaskId++
	return msg
}

// rpc调用，这里会通过*reply将task传递给worker
func (c *Coordinator) AssignTask(args *TaskArgs, reply *Task) error {
	// 分配任务的时候应该上锁，防止多个worker竞争
	mu.Lock()
	defer mu.Unlock()

	//判断当前项目阶段
	switch c.Phase {

	case MapPhase:
		{
			if len(c.TaskChannelMap) > 0 {
				*reply = *<-c.TaskChannelMap
				// 判断给定任务是否在工作，并修改当前任务信息状态
				taskinfo, ok := c.TaskMeta[reply.TaskId]
				if !ok || taskinfo.Taskstate != Waiting {
					return fmt.Errorf("taskid %d is running", reply.TaskId)
				} else {
					taskinfo.Taskstate = Working
					taskinfo.StartTime = time.Now()
				}
				return nil
			} else {
				reply.TaskType = WaittingTask //当前没有任务可以分配给这个worker，他就需要轮询等待

				// 检查是否要跳转到下一个阶段
				if c.checkstate() {
					c.nextparse()
				}

				return nil
			}
		}

	case ReducePhase:
		{
			if len(c.TaskChannelReduce) > 0 {
				*reply = *<-c.TaskChannelReduce
				//fmt.Printf("reply number %d\n", reply.TaskId)
				// 判断给定任务是否在工作，并修改当前任务信息状态
				taskinfo, ok := c.TaskMeta[reply.TaskId]
				if !ok || taskinfo.Taskstate != Waiting {
					return fmt.Errorf("taskid %d is running", reply.TaskId)
				} else {
					taskinfo.Taskstate = Working
					taskinfo.StartTime = time.Now()
				}
				return nil
			} else {
				reply.TaskType = WaittingTask //当前没有任务可以分配给这个worker，他就需要轮询等待

				// 检查是否要跳转到下一个阶段
				if c.checkstate() {
					c.nextparse()
				}

				return nil
			}
		}

	case Alldone:
		{
			reply.TaskType = ExitTask
		}

	default:
		{
			panic("The phase undefined!!!!")
		}

	}
	return nil
}

func (c *Coordinator) checkstate() bool {
	var (
		mapDoneNum      = 0
		mapUnDoneNum    = 0
		reduceDoneNum   = 0
		reduceUnDoneNum = 0
	)

	// 遍历Coordinator中存储的task元信息
	for _, v := range c.TaskMeta {
		if v.TaskPoint.TaskType == MapTask {
			if v.Taskstate == Done {
				mapDoneNum++
			} else {
				mapUnDoneNum++
			}
		} else if v.TaskPoint.TaskType == ReduceTask {
			if v.Taskstate == Done {
				reduceDoneNum++
			} else {
				reduceUnDoneNum++
			}
		}
	}

	// Map阶段完成
	if (mapDoneNum > 0 && mapUnDoneNum == 0) && (reduceDoneNum == 0 && reduceUnDoneNum == 0) {
		return true
	} else {
		// Reduce阶段完成
		if reduceDoneNum > 0 && reduceUnDoneNum == 0 {
			return true
		}
	}
	return false
}

func (c *Coordinator) nextparse() {
	//当map阶段的任务都完成之后，就可以分配reduce阶段的任务到通道里了
	if c.Phase == MapPhase {
		c.createReduceTasks()
		c.Phase = ReducePhase
		//fmt.Printf("%d\n", c.TaskId)
	} else if c.Phase == ReducePhase {
		c.Phase = Alldone
	}
}

func (c *Coordinator) TaskFinished(args *Task, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()

	switch args.TaskType {
	case MapTask:
		{
			meta, ok := c.TaskMeta[args.TaskId]

			if ok && meta.Taskstate == Working {
				// 修改任务状态为完成
				meta.Taskstate = Done
				//fmt.Printf("Map task %d is finished\n", args.TaskId)
			} else {
				//fmt.Printf("Map task %d is already finished!\n", args.TaskId)
			}
		}

	case ReduceTask:
		meta, ok := c.TaskMeta[args.TaskId]

		//prevent a duplicated work which returned from another worker
		if ok && meta.Taskstate == Working {
			// 修改任务状态为完成
			meta.Taskstate = Done
			//fmt.Printf("Reduce task %d is finished\n", args.TaskId)
		} else {
			//fmt.Printf("Reduce task Id[%d] is finished,already!\n", args.TaskId)
		}

	default:
		{
			panic("The task type undefined!")
		}
	}
	return nil
}

func (c *Coordinator) CrashHandler() {
	for {
		time.Sleep(time.Second * 2)
		mu.Lock()
		if c.Phase == Alldone {
			mu.Unlock()
			break
		}

		for _, v := range c.TaskMeta {
			if v.Taskstate == Working && time.Since(v.StartTime) > 9*time.Second {
				switch v.TaskPoint.TaskType {
				case MapTask:
					{
						c.TaskChannelMap <- v.TaskPoint
						v.Taskstate = Waiting
					}
				case ReduceTask:
					{
						c.TaskChannelReduce <- v.TaskPoint
						v.Taskstate = Waiting
					}
				}
			}

		}
		mu.Unlock()
	}
}
