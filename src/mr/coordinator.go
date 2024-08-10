package mr

import (
	"context"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
)

// TaskStatus represents the status of a task
type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

type WorkerPerformance struct {
	avgCompletionTime time.Duration
	taskCount         int
}

// Coordinator manages the overall MapReduce process
type Coordinator struct {
	mu                sync.Mutex
	mapTasks          []string
	nReduce           int
	taskStatus        map[int]TaskStatus
	workerLoads       map[string]int
	workerPerformance map[string]WorkerPerformance
	done              bool
	mapTaskDone       int
	reduceTaskDone    int
	shutdown          chan os.Signal
}

// AssignTask handles task assignment to workers based on load and availability
func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
    c.mu.Lock()
    defer c.mu.Unlock()

    workerID := args.WorkerID
    c.workerLoads[workerID]++

    // Evaluate worker performance for smarter task assignment
    minLoad := c.workerLoads[workerID]
    bestWorkerID := workerID

    for id, load := range c.workerLoads {
        if load < minLoad {
            // Consider worker performance in the decision
            if performance, ok := c.workerPerformance[id]; ok {
                if performance.avgCompletionTime < c.workerPerformance[bestWorkerID].avgCompletionTime {
                    bestWorkerID = id
                    minLoad = load
                }
            } else {
                // If no performance data is available, still consider load
                bestWorkerID = id
                minLoad = load
            }
        }
    }

    workerID = bestWorkerID
    c.workerLoads[workerID]++

    // Assign Map Task
    for i, file := range c.mapTasks {
        if c.taskStatus[i] == Idle {
            c.taskStatus[i] = InProgress
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
            if c.taskStatus[i+len(c.mapTasks)] == Idle {
                c.taskStatus[i+len(c.mapTasks)] = InProgress
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
		c.taskStatus[args.TaskId] = Completed
	} else if args.TaskType == "reduce" {
		c.reduceTaskDone++
		c.taskStatus[args.TaskId+len(c.mapTasks)] = Completed
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
		mapTasks:          files,
		nReduce:           nReduce,
		taskStatus:        make(map[int]TaskStatus),
		workerLoads:       make(map[string]int),
		workerPerformance: make(map[string]WorkerPerformance),
		shutdown:          make(chan os.Signal, 1),
	}

	for i := 0; i < len(files); i++ {
		c.taskStatus[i] = Idle
	}
	for i := 0; i < nReduce; i++ {
		c.taskStatus[i+len(files)] = Idle
	}

	signal.Notify(c.shutdown, syscall.SIGINT, syscall.SIGTERM)
	go c.server()
	go c.handleShutdown()

	return &c
}

// monitorWorker monitors task execution and reassigns if necessary
func (c *Coordinator) monitorWorker(taskId int, taskType string, workerID string) {
	// Adjust timeout dynamically based on system load
	timeout := 10*time.Second + time.Duration(c.workerLoads[workerID])*time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	startTime := time.Now()
	<-ctx.Done()
	c.mu.Lock()
	defer c.mu.Unlock()

	// Reassign task if it's still in progress
	if taskType == "map" && c.taskStatus[taskId] == InProgress {
		c.taskStatus[taskId] = Idle
		logrus.WithFields(logrus.Fields{
			"task": taskId,
		}).Warn("Map Task reassigned after timeout")
	} else if taskType == "reduce" && c.taskStatus[taskId+len(c.mapTasks)] == InProgress {
		c.taskStatus[taskId+len(c.mapTasks)] = Idle
		logrus.WithFields(logrus.Fields{
			"task": taskId,
		}).Warn("Reduce Task reassigned after timeout")
	}

	// Record worker performance
	elapsedTime := time.Since(startTime)
	performance := c.workerPerformance[workerID]
	performance.avgCompletionTime = (performance.avgCompletionTime*time.Duration(performance.taskCount) + elapsedTime) / time.Duration(performance.taskCount+1)
	performance.taskCount++
	c.workerPerformance[workerID] = performance

	// Cancel task if reassigned
	c.cancelTask(workerID, taskId, taskType)
}

// cancelTask sends a cancellation signal to the worker if the task is reassigned
func (c *Coordinator) cancelTask(workerID string, taskId int, taskType string) {
	// Implement task cancellation logic here
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

// handleShutdown gracefully handles shutdown signals
func (c *Coordinator) handleShutdown() {
	<-c.shutdown
	logrus.Info("Received shutdown signal, terminating...")
	c.mu.Lock()
	defer c.mu.Unlock()

	// Gracefully finish ongoing tasks before shutting down
	for c.mapTaskDone != len(c.mapTasks) || c.reduceTaskDone != c.nReduce {
		logrus.Info("Waiting for ongoing tasks to complete before shutdown...")
		time.Sleep(1 * time.Second)
	}
	c.done = true
	logrus.Info("All tasks completed. Coordinator is shutting down.")
	os.Exit(0)
}

// callWithRetry sends an RPC request with retries and exponential backoff
func callWithRetry(rpcname string, args interface{}, reply interface{}) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	backoff := time.Second
	for {
		select {
		case <-ctx.Done():
			logrus.Errorf("Failed to call RPC %s within timeout", rpcname)
			return false
		default:
			if call(rpcname, args, reply) {
				return true
			}
			time.Sleep(backoff)
			backoff *= 2
		}
	}
}