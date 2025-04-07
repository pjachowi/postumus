package impl

import (
	"context"
	"fmt"
	"foobar/postumus/proto"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	gproto "google.golang.org/protobuf/proto"
)

type ThreadSafeWorkflow struct {
	workflow *proto.Workflow
	mutex    sync.RWMutex
}

func (w *ThreadSafeWorkflow) AllCompleted() bool {
	w.mutex.RLock()
	defer w.mutex.RUnlock()
	for _, t := range w.workflow.Tasks {
		if t.Status != proto.Task_COMPLETED && t.Status != proto.Task_FAILED {
			return false
		}
	}
	return true
}

func (w *ThreadSafeWorkflow) UpdateTask(t *proto.Task) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	for i, task := range w.workflow.Tasks {
		if task.Id == t.Id {
			w.workflow.Tasks[i] = t
			break
		}
	}
}

func (w *ThreadSafeWorkflow) TaskFailed(t *proto.Task, readyTasks chan []byte) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	var task *proto.Task
	for _, tt := range w.workflow.Tasks {
		if t.Id == tt.Id {
			task = tt
			break
		}
	}
	if task == nil {
		log.Printf("Task %s not found in workflow %s", t.Id, t.WorkflowId)
		return fmt.Errorf("task %s not found", t.Id)
	}

	if task.Attempts < task.MaxAttempts {
		task.Status = proto.Task_PENDING
		serialized, err := gproto.Marshal(task)
		if err != nil {
			log.Printf("Failed to marshal task %s: %v", task.Id, err)
			return fmt.Errorf("failed to marshal task %s", task.Id)
		}
		select {
		case readyTasks <- serialized:
			log.Printf("Task %s requeued", task.Id)
			return nil
		default:
			log.Printf("Task %s requeue failed", task.Id)
		}
	} else {
		log.Printf("Task %s failed after max attempts", task.Id)
		task.Status = proto.Task_FAILED
		return fmt.Errorf("task %s failed after max attempts", task.Id)
	}
	return nil
}

type MasterServer struct {
	proto.UnimplementedMasterServer
	// Workflows is a map where key is workflow id and value is ThreadSafeWorkflow.
	Workflows sync.Map

	// Ready tasks is a map where key is worker type and value is channel storing tasks
	// of given type, ready to ship to worker.
	ReadyTasks sync.Map

	// Capacity is the maximum number of tasks of given type stored in ReadyTasks.
	Capacity int

	// DoneTasks is a map where key is tasks id and value is channel to signal task completion.
	DoneTasks sync.Map

	// WorkerCheckoutPeriod is the time period after which the master server will check
	// if the worker is still alive and if the task is still being executed.
	// If the worker is not alive, the task will be rescheduled.
	WorkerCheckoutPeriod time.Duration

	// Additional dial options for master. Used in test to setup in-memory connection.
	DialOptions []grpc.DialOption
}

func NewMasterServer(capacity int, workerCheckoutPeriod time.Duration, workerDialOptions ...grpc.DialOption) *MasterServer {
	return &MasterServer{
		Workflows:            sync.Map{},
		ReadyTasks:           sync.Map{},
		Capacity:             capacity,
		DoneTasks:            sync.Map{},
		WorkerCheckoutPeriod: workerCheckoutPeriod,
		DialOptions:          workerDialOptions,
	}
}

func (s *MasterServer) CreateWorkflow(ctx context.Context, req *proto.CreateWorkflowRequest) (*proto.CreateWorkflowResponse, error) {
	id := uuid.New().String()
	thworkflow := &ThreadSafeWorkflow{
		workflow: &proto.Workflow{Id: id, Name: req.Name, Tasks: req.Tasks},
		mutex:    sync.RWMutex{},
	}
	thworkflow.mutex.Lock()
	defer thworkflow.mutex.Unlock()
	s.Workflows.Store(id, thworkflow)
	for i, t := range req.Tasks {
		var ch chan []byte
		t.WorkflowId = id
		t.Id = fmt.Sprintf("%s/%d", id, i)
		t.Status = proto.Task_PENDING
		chAny, ok := s.ReadyTasks.Load(t.Worker)
		if !ok {
			log.Printf("new chan %s\n", t.Worker)
			// ch = make(chan *proto.Task, s.Capacity)
			ch = make(chan []byte, s.Capacity)
			s.ReadyTasks.Store(t.Worker, ch)
		} else {
			ch = chAny.(chan []byte)
		}

		serialized, err := gproto.Marshal(t)
		if err != nil {
			log.Printf("Failed to marshal task %s: %v", t.Id, err)
			return nil, status.Error(codes.Internal, fmt.Sprintf("failed to marshal task %s", t.Id))
		}
		select {
		case ch <- serialized:
			log.Printf("Task %s added to channel %s\n", t.Id, t.Worker)
		default:
			return nil, status.Error(codes.ResourceExhausted, fmt.Sprintf("too many tasks of type %s", t.Worker))
		}

	}
	return &proto.CreateWorkflowResponse{Id: id}, nil
}

func (s *MasterServer) GetWorkflow(ctx context.Context, req *proto.GetWorkflowRequest) (*proto.GetWorkflowResponse, error) {
	tsworkflow, ok := s.Workflows.Load(req.Id)
	if !ok {
		return nil, fmt.Errorf("workflow not found")
	}
	tsworkflow.(*ThreadSafeWorkflow).mutex.RLock()
	defer tsworkflow.(*ThreadSafeWorkflow).mutex.RUnlock()
	workflow := tsworkflow.(*ThreadSafeWorkflow).workflow
	return &proto.GetWorkflowResponse{Workflow: workflow}, nil
}

func (s *MasterServer) GetWorkflowIds(ctx context.Context, req *proto.GetWorkflowIdsRequest) (*proto.GetWorkflowIdsResponse, error) {
	ids := make([]string, 0)
	s.Workflows.Range(func(key, value any) bool {
		ids = append(ids, key.(string))
		return true
	})
	return &proto.GetWorkflowIdsResponse{Ids: ids}, nil
}

func (s *MasterServer) GetTask(ctx context.Context, req *proto.GetTaskRequest) (*proto.GetTaskResponse, error) {
	// log.Printf("GetTask %s", req.Worker)
	notaskResp := &proto.GetTaskResponse{Response: &proto.GetTaskResponse_Notask{Notask: &proto.Notask{}}}
	readyCh, ok := s.ReadyTasks.Load(req.Worker)
	if !ok {
		return notaskResp, nil
	}

	select {
	case serialized := <-readyCh.(chan []byte):
		t := &proto.Task{}
		err := gproto.Unmarshal(serialized, t)
		if err != nil {
			log.Printf("Failed to unmarshal task %s: %v", t.Id, err)
			return nil, status.Error(codes.Internal, fmt.Sprintf("failed to unmarshal task %s", t.Id))
		}
		tsworkflow, ok := s.Workflows.Load(t.WorkflowId)
		if !ok {
			log.Printf("Workflow %s not found for task %s", t.WorkflowId, t.Id)
			return notaskResp, nil
		}
		t.Status = proto.Task_RUNNING
		t.Worker = req.Worker
		t.Attempts++
		tsworkflow.(*ThreadSafeWorkflow).UpdateTask(t)
		done := make(chan struct{})
		s.DoneTasks.Store(t.Id, done)
		// Create goroutine to handle task execution
		go func(t *proto.Task, done chan struct{}) {
			for {
				log.Printf("Tick %sS", t.Id)
				select {
				// TODO introduce delay beetween done and retrying task
				case <-done:
					log.Printf("Task %s completed", t.Id)
					s.DoneTasks.Delete(t.Id)
					return
				case <-time.After(s.WorkerCheckoutPeriod):
					options := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
					if s.DialOptions != nil {
						options = append(options, s.DialOptions...)
					}
					cc, err := grpc.NewClient(req.WorkerUri, options...)
					if err != nil {
						log.Printf("Failed to connect to worker %s: %v", req.WorkerUri, err)
						t.Status = proto.Task_FAILED
						tsworkflow.(*ThreadSafeWorkflow).TaskFailed(t, readyCh.(chan []byte))
						return
					}
					defer cc.Close()
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					client := proto.NewWorkerClient(cc)
					result, err := client.GetCurrentTask(ctx, &proto.GetCurrentTaskRequest{})
					if err != nil {
						log.Printf("Failed to get current task from worker %s: %v", req.WorkerUri, err)
						t.Status = proto.Task_FAILED
						tsworkflow.(*ThreadSafeWorkflow).TaskFailed(t, readyCh.(chan []byte))
						return
					}
					if result.GetNotask() != nil {
						log.Printf("Worker %s is idle", req.WorkerUri)
						t.Status = proto.Task_FAILED
						tsworkflow.(*ThreadSafeWorkflow).TaskFailed(t, readyCh.(chan []byte))
						return
					}
					task := result.GetTask()
					if task == nil {
						log.Printf("Worker %s is idle", req.WorkerUri)
						t.Status = proto.Task_FAILED
						tsworkflow.(*ThreadSafeWorkflow).TaskFailed(t, readyCh.(chan []byte))
						return
					}
					if task.Id != t.Id {
						log.Printf("Task %s is not the current task for worker %s", t.Id, req.WorkerUri)
						t.Status = proto.Task_FAILED
						tsworkflow.(*ThreadSafeWorkflow).TaskFailed(t, readyCh.(chan []byte))
						return
					}
				}

			}
		}(t, done)
		return &proto.GetTaskResponse{Response: &proto.GetTaskResponse_Task{Task: t}}, nil
	default:
		return notaskResp, nil
	}

}

func (s *MasterServer) ReportTaskResult(ctx context.Context, req *proto.ReportTaskResultRequest) (*proto.ReportTaskResultResponse, error) {
	log.Printf("ReportTaskResult %s", req.Task)
	tsworkflow, found := s.Workflows.Load(req.Task.WorkflowId)
	if !found {
		log.Printf("Workflow %s not found for task %s", req.Task.WorkflowId, req.Task.Id)
		return nil, status.Error(codes.NotFound, fmt.Sprintf("workflow %s not found", req.Task.WorkflowId))
	}

	tsworkflow.(*ThreadSafeWorkflow).UpdateTask(req.Task)
	// workflow := tsworkflow.(*ThreadSafeWorkflow).workflow
	// updateTask(workflow, req.Task)
	switch *req.Task.Status.Enum() {
	case proto.Task_FAILED:
		log.Printf("Task %s failed: %v", req.Task.Id, req.Task.Status)
		readyTasks, found := s.ReadyTasks.Load(req.Task.Worker)
		if !found {
			log.Printf("Worker %s not found", req.Task.Worker)
			return nil, status.Error(codes.Internal, fmt.Sprintf("channel for worker %s not found", req.Task.Worker))
		}
		tsworkflow.(*ThreadSafeWorkflow).TaskFailed(req.Task, readyTasks.(chan []byte))
		return &proto.ReportTaskResultResponse{}, nil
	case proto.Task_COMPLETED:
		if tsworkflow.(*ThreadSafeWorkflow).AllCompleted() {
			s.Workflows.Delete(req.Task.WorkflowId)
			log.Printf("Workflow %s completed", req.Task.WorkflowId)
		}
	default:
		log.Printf("Task %s status: %v", req.Task.Id, req.Task.Status)
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("task %s has invalid status %v", req.Task.Id, req.Task.Status))
	}
	return &proto.ReportTaskResultResponse{}, nil
}

func taskWithId(w *proto.Workflow, id string) (t *proto.Task, ok bool) {
	tasks := w.Tasks
	for _, t := range tasks {
		if t.Id == id {
			return t, true
		}
	}
	return nil, false
}
