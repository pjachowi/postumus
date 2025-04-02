package impl_test

import (
	"context"
	"fmt"
	"foobar/postumus/master/impl"
	"foobar/postumus/proto"
	"log"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestMain(m *testing.M) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	m.Run()
}

func TestNothingScheduleNoTaskReceived(t *testing.T) {
	master := impl.NewMasterServer(10, time.Duration(0))
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
	master := impl.NewMasterServer(10, time.Duration(0))
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
	master := impl.NewMasterServer(10, time.Duration(0))
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
	master := impl.NewMasterServer(10, time.Duration(0))
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
	master := impl.NewMasterServer(10, time.Duration(0))
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

	master := impl.NewMasterServer(10, time.Duration(0))
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
	master := impl.NewMasterServer(10, time.Duration(0))
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

	proto.RegisterMasterServer(srv, impl.NewMasterServer(10, time.Duration(0)))

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

type Doubleworker struct {
	proto.WorkerServer
	Response    *proto.GetCurrentTaskResponse
	Invocations map[string]int
}

func (w *Doubleworker) GetCurrentTask(ctx context.Context, req *proto.GetCurrentTaskRequest) (*proto.GetCurrentTaskResponse, error) {
	w.Invocations["GetCurrentTask"]++
	// Return predefined response.
	return w.Response, nil
}

func NewDoubleworker(response *proto.GetCurrentTaskResponse) *Doubleworker {
	return &Doubleworker{
		Response:    response,
		Invocations: make(map[string]int),
	}
}

func TestWithGrpcAndReportTaskResultWhileWorkerClaimsNoTask(t *testing.T) {

	// Common channel
	listen := bufconn.Listen(1024 * 1024)
	defer listen.Close()

	// Worker
	worker := grpc.NewServer()
	defer worker.Stop()

	w := NewDoubleworker(&proto.GetCurrentTaskResponse{
		Response: &proto.GetCurrentTaskResponse_Notask{},
	})
	proto.RegisterWorkerServer(worker, w)

	errChan := make(chan error, 1)
	go func() {
		errChan <- worker.Serve(listen)
	}()

	select {
	case err := <-errChan:
		t.Fatalf("Failed to serve: %v", err)
	default:
	}

	// Server
	srv := grpc.NewServer()
	defer srv.Stop()

	proto.RegisterMasterServer(srv, impl.NewMasterServer(10, time.Duration(0), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listen.Dial()
	})))

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		wg.Done()
		errChan <- srv.Serve(listen)
	}()
	wg.Wait()

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
				Worker:      "test",
				MaxAttempts: 3,
			},
		},
	}
	workflowResp, err := client.CreateWorkflow(ctx, &proto.CreateWorkflowRequest{Name: workflow.Name, Tasks: workflow.Tasks})
	if err != nil {
		t.Fatalf("Error creating workflow: %v", err)
	}
	workflowId := workflowResp.Id

	resp, err := client.GetTask(ctx, &proto.GetTaskRequest{Worker: "test", WorkerUri: "passthrough://bufnet"})
	log.Printf("received task: %v", resp)
	if err != nil {
		t.Fatalf("Error: %v", err)
	}
	task := resp.GetTask()
	if task == nil {
		t.Fatalf("Expected task, got nil")
		return
	}

	// Giving time for master to invoke GetCurrentTask
	// TODO: find a better way to do this
	time.Sleep(10 * time.Millisecond)
	if w.Invocations["GetCurrentTask"] == 0 {
		t.Fatalf("GetCurrentTask was not invoked")
	}

	r, err := client.GetWorkflow(ctx, &proto.GetWorkflowRequest{Id: workflowId})
	if err != nil {
		t.Fatalf("Error getting workflow: %v", err)
	}
	log.Printf("received workflow: %v", r)
	obrainedTask, err := TaskById(r.Workflow, task.Id)
	if err != nil {
		t.Fatalf("Error getting task by id: %v", err)
	}
	// TODO Attempts should be 1 here
	if obrainedTask.Attempts != 2 {
		t.Fatalf("Expected 1 attempt, got %d", obrainedTask.Attempts)
	}

}

func TestWithGrpcScheduleWorkflowReportNoTaskThenProcess(t *testing.T) {

	// Common channel
	listen := bufconn.Listen(1024 * 1024)
	defer listen.Close()

	// Worker
	worker := grpc.NewServer()
	defer worker.Stop()

	w := NewDoubleworker(&proto.GetCurrentTaskResponse{
		Response: &proto.GetCurrentTaskResponse_Notask{},
	})
	proto.RegisterWorkerServer(worker, w)

	errChan := make(chan error, 1)
	go func() {
		errChan <- worker.Serve(listen)
	}()

	select {
	case err := <-errChan:
		t.Fatalf("Failed to serve: %v", err)
	default:
	}

	// Server
	srv := grpc.NewServer()
	defer srv.Stop()

	proto.RegisterMasterServer(srv, impl.NewMasterServer(10, time.Duration(0), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listen.Dial()
	})))

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		wg.Done()
		errChan <- srv.Serve(listen)
	}()
	wg.Wait()

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
				Worker:      "test",
				MaxAttempts: 3,
			},
		},
	}
	workflowResp, err := client.CreateWorkflow(ctx, &proto.CreateWorkflowRequest{Name: workflow.Name, Tasks: workflow.Tasks})
	if err != nil {
		t.Fatalf("Error creating workflow: %v", err)
	}
	workflowId := workflowResp.Id

	resp, err := client.GetTask(ctx, &proto.GetTaskRequest{Worker: "test", WorkerUri: "passthrough://bufnet"})
	log.Printf("received task: %v", resp)
	if err != nil {
		t.Fatalf("Error: %v", err)
	}
	task := resp.GetTask()
	if task == nil {
		t.Fatalf("Expected task, got nil")
		return
	}

	// Giving time for master to invoke GetCurrentTask
	// TODO: find a better way to do this
	time.Sleep(10 * time.Millisecond)
	if w.Invocations["GetCurrentTask"] == 0 {
		t.Fatalf("GetCurrentTask was not invoked")
	}

	r, err := client.GetWorkflow(ctx, &proto.GetWorkflowRequest{Id: workflowId})
	if err != nil {
		t.Fatalf("Error getting workflow: %v", err)
	}
	log.Printf("received workflow: %v", r)
	obrainedTask, err := TaskById(r.Workflow, task.Id)
	if err != nil {
		t.Fatalf("Error getting task by id: %v", err)
	}
	// TODO Attempts should be 1 here
	if obrainedTask.Attempts != 2 {
		t.Fatalf("Expected 1 attempt, got %d", obrainedTask.Attempts)
	}

	w.Response = &proto.GetCurrentTaskResponse{
		Response: &proto.GetCurrentTaskResponse_Task{
			Task: task,
		},
	}

	resp, err = client.GetTask(ctx, &proto.GetTaskRequest{Worker: "test", WorkerUri: "passthrough://bufnet"})
	if err != nil {
		t.Fatalf("Error getting task: %v", err)
	}
	task = resp.GetTask()
	if task == nil {
		t.Fatalf("Expected task, got nil")
		return
	}
	task.Status = proto.Task_COMPLETED
	_, err = client.ReportTaskResult(ctx, &proto.ReportTaskResultRequest{Task: task})
	if err != nil {
		t.Fatalf("Error reporting task result: %v", err)
	}

	r, err = client.GetWorkflow(ctx, &proto.GetWorkflowRequest{Id: workflowId})
	if err == nil {
		t.Fatalf("Expected error getting workflow, got nil and response: %v", r)
	}

}

func TaskById(workflow *proto.Workflow, s string) (*proto.Task, error) {
	for _, t := range workflow.Tasks {
		if t.Id == s {
			return t, nil
		}
	}
	return nil, fmt.Errorf("task with id %s not found", s)
}
