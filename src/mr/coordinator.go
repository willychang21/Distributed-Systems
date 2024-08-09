package mr

import (
	"context"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// Coordinator manages the overall MapReduce process
type Coordinator struct {
	mu             sync.Mutex
	mapTasks       []string
	nReduce        int
	taskStatus     map[int]string
	workerLoads    map[string]int 
	done           bool
	mapTaskDone    int
	reduceTaskDone int
}

// AssignTask handles task assignment to workers based on load and availability
func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	workerID := args.WorkerID
	c.workerLoads[workerID]++

	// Assign Map Task
	for i, file := range c.mapTasks {
		if c.taskStatus[i] == "idle" {
			c.taskStatus[i] = "in-progress"
			reply.TaskType = "map"
			reply.Filename = file
			reply.TaskId = i
			reply.NReduce = c.nReduce
			reply.NMap = len(c.mapTasks)
			logrus.WithFields(logrus.Fields{
				"task":     i,
				"workerID": workerID,
			}).Info("Assigned Map Task to worker")
			go c.monitorWorker(i, "map", workerID)
			return nil
		}
	}

	// Assign Reduce Task
	if c.mapTaskDone == len(c.mapTasks) {
		for i := 0; i < c.nReduce; i++ {
			if c.taskStatus[i+len(c.mapTasks)] == "idle" {
				c.taskStatus[i+len(c.mapTasks)] = "in-progress"
				reply.TaskType = "reduce"
				reply.TaskId = i
				reply.NReduce = c.nReduce
				reply.NMap = len(c.mapTasks)
				logrus.WithFields(logrus.Fields{
					"task":     i,
					"workerID": workerID,
				}).Info("Assigned Reduce Task to worker")
				go c.monitorWorker(i, "reduce", workerID)
				return nil
			}
		}
	}

	// If no tasks available, notify worker to wait
	reply.TaskType = "wait"
	logrus.Info("No tasks available, worker should wait")
	return nil
}

// TaskDone marks a task as completed by a worker
func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	workerID := args.WorkerID
	c.workerLoads[workerID]--

	if args.TaskType == "map" {
		c.mapTaskDone++
		c.taskStatus[args.TaskId] = "completed"
	} else if args.TaskType == "reduce" {
		c.reduceTaskDone++
		c.taskStatus[args.TaskId+len(c.mapTasks)] = "completed"
	}

	if c.mapTaskDone == len(c.mapTasks) && c.reduceTaskDone == c.nReduce {
		c.done = true
	}

	reply.Ack = true
	logrus.WithFields(logrus.Fields{
		"taskType": args.TaskType,
		"taskId":   args.TaskId,
		"workerID": workerID,
	}).Info("Task completed")
	return nil
}

// Done checks if all tasks have been completed
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.done
}

// MakeCoordinator initializes a Coordinator and starts the RPC server
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTasks:    files,
		nReduce:     nReduce,
		taskStatus:  make(map[int]string),
		workerLoads: make(map[string]int),
	}

	for i := 0; i < len(files); i++ {
		c.taskStatus[i] = "idle"
	}
	for i := 0; i < nReduce; i++ {
		c.taskStatus[i+len(files)] = "idle"
	}

	go c.server()
	return &c
}

// monitorWorker monitors task execution and reassigns if necessary
func (c *Coordinator) monitorWorker(taskId int, taskType string, workerID string) {
	// Adjust timeout dynamically based on system load
	timeout := 10*time.Second + time.Duration(c.workerLoads[workerID])*time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	<-ctx.Done()
	c.mu.Lock()
	defer c.mu.Unlock()

	if taskType == "map" && c.taskStatus[taskId] == "in-progress" {
		c.taskStatus[taskId] = "idle"
		logrus.WithFields(logrus.Fields{
			"task": taskId,
		}).Warn("Map Task reassigned after timeout")
	} else if taskType == "reduce" && c.taskStatus[taskId+len(c.mapTasks)] == "in-progress" {
		c.taskStatus[taskId+len(c.mapTasks)] = "idle"
		logrus.WithFields(logrus.Fields{
			"task": taskId,
		}).Warn("Reduce Task reassigned after timeout")
	}

	// Cancel task if reassigned
	c.cancelTask(workerID, taskId, taskType)
}

// cancelTask sends a cancellation signal to the worker if the task is reassigned
func (c *Coordinator) cancelTask(workerID string, taskId int, taskType string) {
	// Implement task cancellation logic here
	// For example, send a cancellation RPC to the worker or remove the task from its queue
	logrus.WithFields(logrus.Fields{
		"workerID": workerID,
		"taskType": taskType,
		"taskId":   taskId,
	}).Info("Task cancellation signal sent to worker")
}

// server starts the RPC server for the Coordinator
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		logrus.Fatalf("listen error: %v", e)
	}
	go http.Serve(l, nil)
}

// coordinatorSock generates a unique UNIX-domain socket name
func coordinatorSock() string {
	return "/var/tmp/5840-mr-" + strconv.Itoa(os.Getuid())
}
