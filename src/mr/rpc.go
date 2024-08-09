package mr

// TaskArgs is the RPC arguments structure.
type TaskArgs struct {
	WorkerID string // Unique identifier for the worker
}

// TaskReply is the RPC reply structure.
type TaskReply struct {
	TaskType string
	Filename string
	TaskId   int
	NReduce  int
	NMap     int
}

// TaskDoneArgs is the structure for reporting task completion.
type TaskDoneArgs struct {
	TaskType string
	TaskId   int
	WorkerID string // Unique identifier for the worker
}

// TaskDoneReply is the reply structure for task completion.
type TaskDoneReply struct {
	Ack bool
}
