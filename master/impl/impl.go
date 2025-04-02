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
	gproto "google.golang.org/protobuf/proto" //nolint:staticcheck
)

type MasterServer struct {
	proto.UnimplementedMasterServer
	Workflows sync.Map
	// Workflows map[string]*proto.Workflow
	// ReadyTasks map[string]*chan (*proto.Task)
	ReadyTasks           sync.Map
	Capacity             int
	DoneTasks            sync.Map
	WorkerCheckoutPeriod time.Duration
	DialOptions          []grpc.DialOption
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
	s.Workflows.Store(id, &proto.Workflow{Id: id, Name: req.Name, Tasks: req.Tasks})
	for i, t := range req.Tasks {
		var ch chan *proto.Task
		t.WorkflowId = id
		t.Id = fmt.Sprintf("%s/%d", id, i)
		t.Status = proto.Task_PENDING
		chAny, ok := s.ReadyTasks.Load(t.Worker)
		if !ok {
			log.Printf("new chan %s\n", t.Worker)
			ch = make(chan *proto.Task, s.Capacity)
			s.ReadyTasks.Store(t.Worker, ch)
		} else {
			ch = chAny.(chan *proto.Task)
		}

		select {
		case ch <- t:
			t.Attempts++
		default:
			return nil, status.Error(codes.ResourceExhausted, fmt.Sprintf("too many tasks of type %s", t.Worker))
		}

	}
	return &proto.CreateWorkflowResponse{Id: id}, nil
}

func (s *MasterServer) GetWorkflow(ctx context.Context, req *proto.GetWorkflowRequest) (*proto.GetWorkflowResponse, error) {
	workflow, ok := s.Workflows.Load(req.Id)
	if !ok {
		return nil, fmt.Errorf("workflow not found")
	}
	return &proto.GetWorkflowResponse{Workflow: workflow.(*proto.Workflow)}, nil
}

func (s *MasterServer) GetWorkflowIds(ctx context.Context, req *proto.GetWorkflowIdsRequest) (*proto.GetWorkflowIdsResponse, error) {
	ids := make([]string, 0)
	s.Workflows.Range(func(key, value interface{}) bool {
		ids = append(ids, key.(string))
		return true
	})
	return &proto.GetWorkflowIdsResponse{Ids: ids}, nil
}

func (s *MasterServer) GetTask(ctx context.Context, req *proto.GetTaskRequest) (*proto.GetTaskResponse, error) {
	log.Printf("GetTask %s", req.Worker)
	notaskResp := &proto.GetTaskResponse{Response: &proto.GetTaskResponse_Notask{Notask: &proto.Notask{}}}
	readyCh, ok := s.ReadyTasks.Load(req.Worker)
	if !ok {
		return notaskResp, nil
	}

	select {
	case t := <-readyCh.(chan *proto.Task):
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
						s.retryTask(t)
						return
					}
					defer cc.Close()
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					client := proto.NewWorkerClient(cc)
					result, err := client.GetCurrentTask(ctx, &proto.GetCurrentTaskRequest{})
					if err != nil {
						log.Printf("Failed to get current task from worker %s: %v", req.WorkerUri, err)
						s.retryTask(t)
						return
					}
					if result.GetNotask() != nil {
						log.Printf("Worker %s is idle", req.WorkerUri)
						s.retryTask(t)
						return
					}
					task := result.GetTask()
					if task == nil {
						log.Printf("Worker %s is idle", req.WorkerUri)
						s.retryTask(t)
						return
					}
					if task.Id != t.Id {
						log.Printf("Task %s is not the current task for worker %s", t.Id, req.WorkerUri)
						s.retryTask(t)
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

func (s *MasterServer) retryTask(t *proto.Task) {
	workflow, ok := s.Workflows.Load(t.WorkflowId)
	if !ok {
		log.Printf("Workflow %s not found for task %s", t.WorkflowId, t.Id)
		return
	}
	origin, found := taskWithId(workflow.(*proto.Workflow).Tasks, t.Id)
	if !found {
		log.Printf("Task %s not found in workflow %s", t.Id, t.WorkflowId)
		return
	}

	if t.Attempts < t.MaxAttempts {
		t.Status = proto.Task_PENDING
		gproto.Merge(origin, t)
		chAny, ok := s.ReadyTasks.Load(t.Worker)
		if !ok {
			log.Printf("Worker %s not found", t.Worker)
			return
		}
		ch := chAny.(chan *proto.Task)
		select {
		case ch <- t:
			t.Attempts++
			log.Printf("Task %s requeued", t.Id)
			return
		default:
			log.Printf("Task %s requeue failed", t.Id)
			return
		}
	} else {
		log.Printf("Task %s failed after max attempts", t.Id)
		t.Status = proto.Task_FAILED
		gproto.Merge(origin, t)
		allCompleted := true
		for _, t := range workflow.(*proto.Workflow).Tasks {
			if t.Status != proto.Task_COMPLETED && t.Status != proto.Task_FAILED {
				allCompleted = false
				break
			}
		}
		if allCompleted {
			log.Printf("Workflow %s completed", t.WorkflowId)
			s.Workflows.Delete(t.WorkflowId)
		}
	}
}

func (s *MasterServer) ReportTaskResult(ctx context.Context, req *proto.ReportTaskResultRequest) (*proto.ReportTaskResultResponse, error) {
	switch *req.Task.Status.Enum() {
	case proto.Task_FAILED:
		log.Printf("Task %s failed: %v", req.Task.Id, req.Task.Status)
		s.taskFailed(req.Task)
		return nil, status.Error(codes.Internal, fmt.Sprintf("task %s failed", req.Task.Id))
	case proto.Task_COMPLETED:
		s.taskCompleted(req.Task)
	default:
		log.Printf("Task %s status: %v", req.Task.Id, req.Task.Status)
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("task %s has invalid status %v", req.Task.Id, req.Task.Status))
	}
	return &proto.ReportTaskResultResponse{}, nil
}

func (s *MasterServer) taskCompleted(task *proto.Task) {
	workflow, ok := s.Workflows.Load(task.WorkflowId)
	if !ok {
		log.Printf("Workflow %s not found for task %s", task.WorkflowId, task.Id)
		return
	}
	allCompleted := true
	for _, t := range workflow.(*proto.Workflow).Tasks {
		if t.Id == task.Id {
			t = task
			break
		}
		if t.Status != proto.Task_COMPLETED {
			allCompleted = false
		}
	}
	if allCompleted {
		s.Workflows.Delete(task.WorkflowId)
	}
	log.Printf("Task %s completed", task.Id)
}

func (s *MasterServer) taskFailed(task *proto.Task) {
	workflow, ok := s.Workflows.Load(task.WorkflowId)
	if !ok {
		log.Printf("Workflow %s not found for task %s", task.WorkflowId, task.Id)
		return
	}
	t, found := taskWithId(workflow.(*proto.Workflow).Tasks, task.Id)
	if !found {
		log.Printf("Task %s not found in workflow %s", task.Id, task.WorkflowId)
		return
	}

	if t.Attempts < t.MaxAttempts {
		t.Status = proto.Task_PENDING
		if chAny, ok := s.ReadyTasks.Load(t.Worker); ok {
			ch := chAny.(chan *proto.Task)
			select {
			case ch <- t:
				t.Attempts++
				log.Printf("Task %s requeued", task.Id)
				return
			default:
				log.Printf("Task %s requeue failed", task.Id)
			}
		}
	} else {
		log.Printf("Task %s failed after max attempts", task.Id)
		s.Workflows.Delete(task.WorkflowId)
	}
}

func taskWithId(tasks []*proto.Task, id string) (t *proto.Task, ok bool) {
	for _, t := range tasks {
		if t.Id == id {
			return t, true
		}
	}
	return nil, false
}
