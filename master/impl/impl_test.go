package impl_test

import (
	"context"
	"foobar/postumus/master/impl"
	"foobar/postumus/proto"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestNothingScheduleNoTaskReceived(t *testing.T) {
	master := impl.NewMasterServer(10)
	resp, err := master.GetTask(context.TODO(), &proto.GetTaskRequest{Worker: "test"})
	if err != nil {
		t.Errorf("Error: %v", err)
	}

	nt := resp.GetNotask()
	if nt == nil {
		t.Errorf("Expected Notask, got nil")
	}
}

func TestScheduledTaskReceivedTask(t *testing.T) {
	master := impl.NewMasterServer(10)
	workflow := &proto.Workflow{
		Name: "test",
		Tasks: []*proto.Task{
			{
				Worker: "test",
			},
		},
	}
	master.CreateWorkflow(context.TODO(), &proto.CreateWorkflowRequest{Name: workflow.Name, Tasks: workflow.Tasks})

	resp, err := master.GetTask(context.TODO(), &proto.GetTaskRequest{Worker: "test"})
	if err != nil {
		t.Errorf("Error: %v", err)
	}

	task := resp.GetTask()
	if task == nil {
		t.Errorf("Expected task, got nil")
	}
}

func TestScheduledTooManyTasksReturnsError(t *testing.T) {
	master := impl.NewMasterServer(10)
	workflow := &proto.Workflow{
		Name: "test",
		Tasks: []*proto.Task{
			{
				Worker: "test",
			},
		},
	}
	for range 10 {
		_, err := master.CreateWorkflow(context.TODO(), &proto.CreateWorkflowRequest{Name: workflow.Name, Tasks: workflow.Tasks})
		if err != nil {
			t.Errorf("Expected workflow scheduled, got %v", err)
		}
	}
	_, err := master.CreateWorkflow(context.TODO(), &proto.CreateWorkflowRequest{Name: workflow.Name, Tasks: workflow.Tasks})
	if err == nil {
		t.Errorf("Expected error during workflow schedule, got nil")
	}
}

func TestSchedueWorkflowAndReportTaskResult(t *testing.T) {
	master := impl.NewMasterServer(10)
	workflow := &proto.Workflow{
		Name: "test",
		Tasks: []*proto.Task{
			{
				Worker: "test",
			},
		},
	}
	master.CreateWorkflow(context.TODO(), &proto.CreateWorkflowRequest{Name: workflow.Name, Tasks: workflow.Tasks})

	resp, err := master.GetTask(context.TODO(), &proto.GetTaskRequest{Worker: "test"})
	if err != nil {
		t.Errorf("Error: %v", err)
	}

	task := resp.GetTask()
	if task == nil {
		t.Fatalf("Expected task, got nil")
		return
	}

	task.Status = proto.Task_COMPLETED
	_, err = master.ReportTaskResult(context.TODO(), &proto.ReportTaskResultRequest{Task: task})
	if err != nil {
		t.Errorf("Error reporting task result: %v", err)
	}

	resp, err = master.GetTask(context.TODO(), &proto.GetTaskRequest{Worker: "test"})
	if err != nil {
		t.Fatalf("Error: %v", err)
		return
	}

	task = resp.GetTask()
	if task != nil {
		t.Errorf("Expected nil, got %v", task)
	}

	workflowIds, err := master.GetWorkflowIds(context.TODO(), &proto.GetWorkflowIdsRequest{})
	if err != nil {
		t.Errorf("Error getting workflow IDs: %v", err)
	}

	if len(workflowIds.Ids) != 0 {
		t.Errorf("Expected zero length for Worker_GetTask_FullMethodName, got %d", len(workflowIds.Ids))
	}
}

func TestScheduleAndGetTaskConcurrent(t *testing.T) {
	master := impl.NewMasterServer(10)
	for range [10]int{} {
		go func() {
			workflow := &proto.Workflow{
				Name: "test",
				Tasks: []*proto.Task{
					{
						Worker: "test",
					},
				},
			}
			master.CreateWorkflow(context.TODO(), &proto.CreateWorkflowRequest{Name: workflow.Name, Tasks: workflow.Tasks})
		}()
	}

	for range [10]int{} {
		go func() {
			_, err := master.GetTask(context.TODO(), &proto.GetTaskRequest{Worker: "test"})
			if err != nil {
				t.Errorf("Error: %v", err)
			}
		}()
	}
}

func TestScheduledAndReturnFailedTask(t *testing.T) {
	const numAttempts = 3

	master := impl.NewMasterServer(10)
	workflow := &proto.Workflow{
		Name: "test",
		Tasks: []*proto.Task{
			{
				Worker:      "test",
				MaxAttempts: numAttempts,
			},
		},
	}
	master.CreateWorkflow(context.TODO(), &proto.CreateWorkflowRequest{Name: workflow.Name, Tasks: workflow.Tasks})

	for range numAttempts {
		resp, err := master.GetTask(context.TODO(), &proto.GetTaskRequest{Worker: "test"})
		if err != nil {
			t.Errorf("Error: %v", err)
		}

		task := resp.GetTask()
		if task == nil {
			t.Fatalf("Expected task, got nil")
			return
		}

		task.Status = proto.Task_FAILED
		_, err = master.ReportTaskResult(context.TODO(), &proto.ReportTaskResultRequest{Task: task})
		if err == nil {
			t.Errorf("Expected error reporting task result, got nil")
		}
	}
	resp, err := master.GetTask(context.TODO(), &proto.GetTaskRequest{Worker: "test"})
	if err != nil {
		t.Errorf("Error: %v", err)
	}
	if resp.GetNotask() == nil {
		t.Errorf("Expected notask, got %v", resp.GetNotask())
	}
}
func TestScheduledAndReturnTaskWithUnknownStatus(t *testing.T) {
	master := impl.NewMasterServer(10)
	workflow := &proto.Workflow{
		Name: "test",
		Tasks: []*proto.Task{
			{
				Worker: "test",
			},
		},
	}
	master.CreateWorkflow(context.TODO(), &proto.CreateWorkflowRequest{Name: workflow.Name, Tasks: workflow.Tasks})

	resp, err := master.GetTask(context.TODO(), &proto.GetTaskRequest{Worker: "test"})
	if err != nil {
		t.Errorf("Error: %v", err)
	}

	task := resp.GetTask()
	if task == nil {
		t.Fatalf("Expected task, got nil")
		return
	}

	task.Status = proto.Task_UNKNOWN
	_, err = master.ReportTaskResult(context.TODO(), &proto.ReportTaskResultRequest{Task: task})
	if err == nil {
		t.Errorf("Expected error reporting task result, got nil")
	}
}

func TestWithGrpc(t *testing.T) {
	// Server
	listen := bufconn.Listen(1024 * 1024)
	defer listen.Close()

	srv := grpc.NewServer()
	defer srv.Stop()

	proto.RegisterMasterServer(srv, impl.NewMasterServer(10))

	errChan := make(chan error, 1)
	go func() {
		errChan <- srv.Serve(listen)
	}()

	select {
	case err := <-errChan:
		t.Fatalf("Failed to serve: %v", err)
	default:
	}

	// Client
	dialer := func(context.Context, string) (net.Conn, error) {
		return listen.Dial()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	conn, err := grpc.NewClient("passthrough://bufnet", grpc.WithContextDialer(dialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.DialContext %v", err)
	}
	defer conn.Close()

	client := proto.NewMasterClient(conn)
	workflow := &proto.Workflow{
		Name: "test",
		Tasks: []*proto.Task{
			{
				Worker: "test",
			},
		},
	}
	_, err = client.CreateWorkflow(ctx, &proto.CreateWorkflowRequest{Name: workflow.Name, Tasks: workflow.Tasks})

	if err != nil {
		t.Fatalf("Error creating workflow: %v", err)
	}
	resp, err := client.GetTask(ctx, &proto.GetTaskRequest{Worker: "test"})
	if err != nil {
		t.Fatalf("Error: %v", err)
	}
	task := resp.GetTask()
	if task == nil {
		t.Fatalf("Expected task, got nil")
		return
	}

}
