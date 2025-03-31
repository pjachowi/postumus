package impl

import (
	"context"
	"fmt"
	"foobar/postumus/proto"
	"log"
	"sync"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type MasterServer struct {
	proto.UnimplementedMasterServer
	Workflows sync.Map
	// Workflows map[string]*proto.Workflow
	// ReadyTasks map[string]*chan (*proto.Task)
	ReadyTasks sync.Map
	capacity   int
}

func NewMasterServer(capacity int) *MasterServer {
	return &MasterServer{
		Workflows:  sync.Map{},
		ReadyTasks: sync.Map{},
		capacity:   capacity,
	}
}

func (s *MasterServer) CreateWorkflow(ctx context.Context, req *proto.CreateWorkflowRequest) (*proto.CreateWorkflowResponse, error) {
	id := uuid.New().String()
	s.Workflows.Store(id, &proto.Workflow{Id: id, Name: req.Name, Tasks: req.Tasks})
	for _, t := range req.Tasks {
		var ch chan *proto.Task
		t.WorkflowId = id
		t.Status = proto.Task_PENDING
		if chAny, ok := s.ReadyTasks.Load(t.Worker); !ok {
			log.Printf("new chan %s\n", t.Worker)
			ch = make(chan *proto.Task, s.capacity)
			s.ReadyTasks.Store(t.Worker, ch)
		} else {
			ch = chAny.(chan *proto.Task)
		}

		select {
		case ch <- t:
			t.Attempts++
			return &proto.CreateWorkflowResponse{Id: id}, nil
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
	notaskResp := &proto.GetTaskResponse{Response: &proto.GetTaskResponse_Notask{Notask: &proto.Notask{}}}
	ch, ok := s.ReadyTasks.Load(req.Worker)
	if !ok {
		return notaskResp, nil
	}

	select {
	case t := <-ch.(chan *proto.Task):
		return &proto.GetTaskResponse{Response: &proto.GetTaskResponse_Task{Task: t}}, nil
	default:
		return notaskResp, nil
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
