package mr

import (
	"context"
	"fmt"
	"net/rpc"
	"os"
	"time"

	"github.com/sirupsen/logrus"
)

// KeyValue represents a key-value pair.
type KeyValue struct {
	Key   string
	Value string
}

// Worker is the entry point for a MapReduce worker.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	workerID := fmt.Sprintf("%d", os.Getpid()) // Use worker's PID as a unique identifier

	for {
		args := TaskArgs{WorkerID: workerID}
		reply := TaskReply{}

		ok := callWithRetry("Coordinator.AssignTask", &args, &reply)
		if !ok {
			logrus.Warn("Failed to get task from Coordinator")
			time.Sleep(time.Second)
			continue
		}

		switch reply.TaskType {
		case "map":
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := doMapTask(ctx, reply.Filename, reply.TaskId, reply.NReduce, mapf); err != nil {
				logrus.WithFields(logrus.Fields{
					"taskType": "map",
					"taskId":   reply.TaskId,
				}).Errorf("Failed to complete task: %v", err)
			}
			cancel() 
			taskDone(reply.TaskType, reply.TaskId, workerID)
		case "reduce":
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := doReduceTask(ctx, reply.TaskId, reply.NMap, reducef); err != nil {
				logrus.WithFields(logrus.Fields{
					"taskType": "reduce",
					"taskId":   reply.TaskId,
				}).Errorf("Failed to complete task: %v", err)
			}
			cancel()
			taskDone(reply.TaskType, reply.TaskId, workerID)
		case "wait":
			time.Sleep(time.Second)
		}
	}
}


// doMapTask performs the Map task.
func doMapTask(ctx context.Context, filename string, taskId int, nReduce int, mapf func(string, string) []KeyValue) error {
	content, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("cannot open %v: %w", filename, err)
	}
	kva := mapf(filename, string(content))

	buckets := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		bucket := ihash(kv.Key) % nReduce
		buckets[bucket] = append(buckets[bucket], kv)
	}

	for i := 0; i < nReduce; i++ {
		select {
		case <-ctx.Done():
			return fmt.Errorf("task %d canceled: %w", taskId, ctx.Err())
		default:
			oname := fmt.Sprintf("mr-%d-%d", taskId, i)
			file, err := os.Create(oname)
			if err != nil {
				return fmt.Errorf("cannot create %v: %w", oname, err)
			}
			defer file.Close()
			if err := writeToJsonFile(file, buckets[i]); err != nil {
				return fmt.Errorf("cannot write to file %v: %w", oname, err)
			}
			logrus.WithFields(logrus.Fields{
				"taskId": taskId,
				"file":   oname,
			}).Info("Map task completed and file created")
		}
	}
	return nil
}

// doReduceTask performs the Reduce task.
func doReduceTask(ctx context.Context, taskId int, nMap int, reducef func(string, []string) string) error {
	intermediate := []KeyValue{}
	for i := 0; i < nMap; i++ {
		select {
		case <-ctx.Done():
			return fmt.Errorf("task %d canceled: %w", taskId, ctx.Err())
		default:
			filename := fmt.Sprintf("mr-%d-%d", i, taskId)
			file, err := os.Open(filename)
			if err != nil {
				return fmt.Errorf("cannot open %v: %w", filename, err)
			}
			defer file.Close()
			intermediate = append(intermediate, readFromJsonFile(file)...)
		}
	}

	merged := make(map[string][]string)
	for _, kv := range intermediate {
		merged[kv.Key] = append(merged[kv.Key], kv.Value)
	}

	oname := fmt.Sprintf("mr-out-%d", taskId)
	ofile, err := os.Create(oname)
	if err != nil {
		return fmt.Errorf("cannot create %v: %w", oname, err)
	}
	defer ofile.Close()

	for key, values := range merged {
		select {
		case <-ctx.Done():
			return fmt.Errorf("task %d canceled: %w", taskId, ctx.Err())
		default:
			output := reducef(key, values)
			fmt.Fprintf(ofile, "%v %v\n", key, output)
		}
	}
	logrus.WithFields(logrus.Fields{
		"taskId": taskId,
		"file":   oname,
	}).Info("Reduce task completed and file created")
	return nil
}

// taskDone reports task completion to the Coordinator.
func taskDone(taskType string, taskId int, workerID string) {
	args := TaskDoneArgs{TaskType: taskType, TaskId: taskId, WorkerID: workerID}
	reply := TaskDoneReply{}
	callWithRetry("Coordinator.TaskDone", &args, &reply)
}

// call sends an RPC request to the Coordinator and waits for the response
func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	var c *rpc.Client
	var err error

	for i := 0; i < 5; i++ {
		c, err = rpc.DialHTTP("unix", sockname)
		if err == nil {
			break
		}
		logrus.WithFields(logrus.Fields{
			"attempt": i + 1,
		}).Warnf("Dialing failed: %v", err)
		time.Sleep(2 * time.Second)
	}

	if err != nil {
		logrus.Fatalf("Dialing failed: %v", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	logrus.WithFields(logrus.Fields{
		"rpcname": rpcname,
	}).Errorf("RPC call error: %v", err)
	return false
}