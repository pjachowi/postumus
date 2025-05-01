package impl_test

import (
	"context"
	"fmt"
	"foobar/postumus/master/impl"
	"foobar/postumus/proto"
	"log"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	gproto "google.golang.org/protobuf/proto"
	//nolint:staticcheck
)

func TestMain(m *testing.M) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	m.Run()
}

func TestNothingScheduleNoTaskReceived(t *testing.T) {
	master := impl.NewMasterServer(10, time.Duration(0), time.Duration(0))
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
	master := impl.NewMasterServer(10, time.Duration(0), time.Duration(0))
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
	master := impl.NewMasterServer(10, time.Duration(0), time.Duration(0))
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
	master := impl.NewMasterServer(10, time.Duration(0), time.Duration(0))
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
	master := impl.NewMasterServer(10, time.Duration(0), time.Duration(0))
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

	master := impl.NewMasterServer(10, time.Duration(10*time.Second), time.Duration(10*time.Second))
	workflow := &proto.Workflow{
		Name: "test",
		Tasks: []*proto.Task{
			{
				Worker:      "test",
				MaxAttempts: numAttempts,
			},
		},
	}
	master.CreateWorkflow(context.Background(), &proto.CreateWorkflowRequest{Name: workflow.Name, Tasks: workflow.Tasks})

	for range numAttempts {
		resp, err := master.GetTask(context.Background(), &proto.GetTaskRequest{Worker: "test"})
		if err != nil {
			t.Errorf("Error: %v", err)
		}

		task := resp.GetTask()
		if task == nil {
			t.Fatalf("Expected task, got nil")
			return
		}

		task.Status = proto.Task_FAILED
		_, err = master.ReportTaskResult(context.Background(), &proto.ReportTaskResultRequest{Task: task})
		if err != nil {
			t.Errorf("Error reporting task result: %v", err)
		}
	}
	resp, err := master.GetTask(context.Background(), &proto.GetTaskRequest{Worker: "test"})
	if err != nil {
		t.Errorf("Error: %v", err)
	}
	if resp.GetNotask() == nil {
		t.Errorf("Expected notask, got %v", resp)
	}
}
func TestScheduledAndReturnTaskWithUnknownStatus(t *testing.T) {
	master := impl.NewMasterServer(10, time.Duration(0), time.Duration(0))
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

	proto.RegisterMasterServer(srv, impl.NewMasterServer(10, time.Duration(0), time.Duration(0)))

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
	M           sync.Mutex
}

type TestWorker struct {
	proto.UnimplementedWorkerServer
	current *proto.Task
	id      string
	mu      sync.Mutex
}

func NewTestWorker(id string) *TestWorker {
	return &TestWorker{id: id}
}

func (w *TestWorker) Run(master proto.MasterClient, done chan any) {
	// defer close(done)
	for {
		log.Printf("[Worker %s] getting task", w.id)
		result, err := master.GetTask(context.Background(), &proto.GetTaskRequest{
			Worker:    "test",
			WorkerUri: "passthrough://bufnet",
		})
		log.Printf("[Worker %s] got task result: %v", w.id, result)
		if err != nil {
			log.Printf("[Worker %s] Error getting task: %v", w.id, err)
			return
		}
		if result.GetNotask() != nil {
			log.Printf("[Worker %s] Got notask. sleeping 100 ms or done", w.id)
			select {
			case <-done:
				log.Printf("[Worker %s] stopped", w.id)
				return
			case <-time.After(100 * time.Millisecond):
				continue
			}

		} else {
			task := result.GetTask()
			if task == nil {
				log.Printf("[Worker %s] Expected task, got nil", w.id)
				return
			}
			log.Printf("[Worker %s] got task: %v", w.id, task)
			w.mu.Lock()
			w.current = gproto.CloneOf(task)
			w.mu.Unlock()
			task.Status = proto.Task_COMPLETED
			log.Printf("[Worker %s] reporting task result: %v", w.id, task)
			_, err = master.ReportTaskResult(context.Background(), &proto.ReportTaskResultRequest{Task: task})
			if err != nil {
				log.Printf("[Worker %s] Error reporting task result: %v", w.id, err)
				return
			}
		}
		w.mu.Lock()
		w.current = nil
		w.mu.Unlock()
	}
}

func (w *TestWorker) GetCurrentTask(ctx context.Context, req *proto.GetCurrentTaskRequest) (*proto.GetCurrentTaskResponse, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.current == nil {
		return &proto.GetCurrentTaskResponse{
			Response: &proto.GetCurrentTaskResponse_Notask{},
		}, nil
	}
	return &proto.GetCurrentTaskResponse{
		Response: &proto.GetCurrentTaskResponse_Task{
			Task: w.current,
		},
	}, nil
}

func (w *Doubleworker) GetCurrentTask(ctx context.Context, req *proto.GetCurrentTaskRequest) (*proto.GetCurrentTaskResponse, error) {
	log.Printf("DoubleWorker: GetCurrentTask returning: %v", w.Response)
	w.M.Lock()
	defer w.M.Unlock()
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

	master := impl.NewMasterServer(
		10,
		time.Duration(0),
		time.Duration(0),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listen.Dial()
		}),
	)
	proto.RegisterMasterServer(srv, master)

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
	time.Sleep(100 * time.Millisecond)
	w.M.Lock()
	if w.Invocations["GetCurrentTask"] == 0 {
		t.Fatalf("GetCurrentTask was not invoked")
	}
	w.M.Unlock()

	r, err := client.GetWorkflow(ctx, &proto.GetWorkflowRequest{Id: workflowId})
	if err != nil {
		t.Fatalf("Error getting workflow: %v", err)
	}
	log.Printf("received workflow: %v", r)
	obrainedTask, err := TaskById(r.Workflow, task.Id)
	if err != nil {
		t.Fatalf("Error getting task by id: %v", err)
	}
	if obrainedTask.Attempts != 1 {
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

	taskCheckChannel := make(chan any)
	taskCheckDone := make(chan any)
	master := impl.NewMasterServerForTests(
		10,
		taskCheckChannel,
		taskCheckDone,
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listen.Dial()
		}),
	)
	proto.RegisterMasterServer(srv, master)

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
	log.Printf("task: %v", task)

	// Trigger worker check
	taskCheckChannel <- true
	// Wait for worker check to finish
	<-taskCheckDone

	r, err := client.GetWorkflow(ctx, &proto.GetWorkflowRequest{Id: workflowId})
	if err != nil {
		t.Fatalf("Error getting workflow: %v", err)
	}
	log.Printf("received workflow: %v", r)
	obrainedTask, err := TaskById(r.Workflow, task.Id)
	if err != nil {
		t.Fatalf("Error getting task by id: %v", err)
	}
	if obrainedTask.Attempts != 1 {
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

func TestGrpcOneWorkflowMultipleTasks(t *testing.T) {
	// Common channel
	listen := bufconn.Listen(1024 * 1024)
	defer listen.Close()

	// Server
	srv := grpc.NewServer()
	defer srv.Stop()
	master := impl.NewMasterServer(
		10,
		time.Duration(10*time.Second),
		time.Duration(10*time.Second),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listen.Dial()
		}),
	)
	proto.RegisterMasterServer(srv, master)
	go srv.Serve(listen)

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

	const numTasks = 10
	workflow := &proto.Workflow{
		Name: "test",
	}
	for i := 0; i < numTasks; i++ {
		workflow.Tasks = append(workflow.Tasks, &proto.Task{
			Name:        fmt.Sprintf("Task %d", i),
			Worker:      "test",
			MaxAttempts: 3,
		})
	}
	resp, err := client.CreateWorkflow(ctx, &proto.CreateWorkflowRequest{Name: workflow.Name, Tasks: workflow.Tasks})
	if err != nil {
		t.Fatalf("Error creating workflow: %v", err)
	}
	workflowId := resp.Id
	log.Printf("Workflow ID: %s", workflowId)

	// Run workers
	done := make([]chan any, numTasks)
	for i := range done {
		worker := grpc.NewServer()
		defer worker.Stop()
		w := NewTestWorker(fmt.Sprintf("test-%d", i))
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
		done[i] = make(chan any)
		go w.Run(client, done[i])
	}

	// Wait for workflow to finish
	for {
		w, err := client.GetWorkflow(ctx, &proto.GetWorkflowRequest{Id: workflowId})
		if err != nil {
			log.Printf("Workflow done")
			break
		}
		unfinshedTask := []string{}
		for _, t := range w.Workflow.Tasks {
			if t.Status != proto.Task_COMPLETED {
				unfinshedTask = append(unfinshedTask, t.Name)
			}
		}
		log.Printf("Workflow still running %s, unfinished tasks: %v", w.Workflow.Id, unfinshedTask)
		time.Sleep(100 * time.Millisecond)
	}

	// Shuting down workers

	log.Printf("Shutting down workers")
	time.Sleep(1 * time.Second)
	for i := range numTasks {
		go func(i int) {
			log.Printf("Shutting down worker %d", i)
			done[i] <- struct{}{}
			log.Printf("Worker %d shut down", i)
		}(i)
	}
	log.Printf("Workers shut down")

}

func TestTaskDepencencies(t *testing.T) {

	workflow := &proto.Workflow{
		Tasks: []*proto.Task{
			{Name: "Task 4", Worker: "test", DependsOn: []string{"Task 2", "Task 3"}},
			{Name: "Task 3", Worker: "test", DependsOn: []string{"Task 2"}},
			{Name: "Task 2", Worker: "test", DependsOn: []string{"Task 1"}},
			{Name: "Task 1", Worker: "test"},
		},
	}

	master := impl.NewMasterServer(10, time.Duration(0), time.Duration(0))
	_, err := master.CreateWorkflow(context.Background(), &proto.CreateWorkflowRequest{Name: "test", Tasks: workflow.Tasks})
	if err != nil {
		t.Fatalf("Error creating workflow: %v", err)
	}
	resp, err := master.GetTask(context.Background(), &proto.GetTaskRequest{Worker: "test"})
	if err != nil {
		t.Fatalf("Error: %v", err)
	}
	task1 := resp.GetTask()
	if task1 == nil {
		t.Fatalf("Expected a task but got nil")
	}
	if task1.Name != "Task 1" {
		t.Errorf("Expected task 1 but got %s", task1.Name)
	}
	task1.Status = proto.Task_COMPLETED
	_, err = master.ReportTaskResult(context.Background(), &proto.ReportTaskResultRequest{Task: task1})
	if err != nil {
		t.Fatalf("Error reporting task result: %v", err)
	}

	for range 2 {
		resp, err = master.GetTask(context.Background(), &proto.GetTaskRequest{Worker: "test"})
		if err != nil {
			t.Fatalf("Error: %v", err)
		}
		task23 := resp.GetTask()
		if task23 == nil {
			t.Fatalf("Expected a task but got nil")
		}
		if task23.Name != "Task 2" && task23.Name != "Task 3" {
			t.Errorf("Expected task 2 or 3 but got %s", task23.Name)
		}
		task23.Status = proto.Task_COMPLETED
		_, err = master.ReportTaskResult(context.Background(), &proto.ReportTaskResultRequest{Task: task23})
		if err != nil {
			t.Fatalf("Error reporting task result: %v", err)
		}
	}
	resp, err = master.GetTask(context.Background(), &proto.GetTaskRequest{Worker: "test"})
	if err != nil {
		t.Fatalf("Error: %v", err)
	}
	task4 := resp.GetTask()
	if task4 == nil {
		t.Fatalf("Expected a task but got nil")
	}
	if task4.Name != "Task 4" {
		t.Errorf("Expected task 4 but got %s", task4.Name)
	}
	task4.Status = proto.Task_COMPLETED
	_, err = master.ReportTaskResult(context.Background(), &proto.ReportTaskResultRequest{Task: task4})
	if err != nil {
		t.Fatalf("Error reporting task result: %v", err)
	}

	resp, err = master.GetTask(context.Background(), &proto.GetTaskRequest{Worker: "test"})
	if err != nil {
		t.Fatalf("Error: %v", err)
	}
	taskNone := resp.GetNotask()
	if taskNone == nil {
		t.Fatalf("Expected no task but got a task")
	}

}

func TestTopologicalOrer(t *testing.T) {
	workflow := &proto.Workflow{
		Tasks: []*proto.Task{
			{Name: "Task 4", Worker: "test", DependsOn: []string{"Task 2", "Task 3"}},
			{Name: "Task 3", Worker: "test", DependsOn: []string{"Task 1"}},
			{Name: "Task 2", Worker: "test", DependsOn: []string{"Task 1"}},
			{Name: "Task 1", Worker: "test"},
		},
	}
	order, err := impl.TopologicalOrder(workflow.Tasks)
	if err != nil {
		t.Fatalf("Error getting topological order: %v", err)
	}
	expectedOrder1 := []int32{3, 2, 1, 0}
	expectedOrder2 := []int32{3, 1, 2, 0}

	if !reflect.DeepEqual(order, expectedOrder1) && !reflect.DeepEqual(order, expectedOrder2) {
		t.Errorf("Expected order %v or %v but got %v", expectedOrder1, expectedOrder2, order)
	}
}

func TesTaskNamesAreUnique(t *testing.T) {
	workflow := &proto.Workflow{
		Tasks: []*proto.Task{
			{Name: "Task 1", Worker: "test"},
			{Name: "Task 1", Worker: "test"},
			{Name: "Task 2", Worker: "test"},
		},
	}
	master := impl.NewMasterServer(10, time.Duration(0), time.Duration(0))
	_, err := master.CreateWorkflow(context.Background(), &proto.CreateWorkflowRequest{Name: "test", Tasks: workflow.Tasks})
	if err == nil {
		t.Errorf("Expected workflow creation fails because of repeated task names, got: %v", err)
	}
}

func TestNoCycleInTasks(t *testing.T) {
	workflow := &proto.Workflow{
		Tasks: []*proto.Task{
			{Name: "Task 1", Worker: "test", DependsOn: []string{"Task 3"}},
			{Name: "Task 2", Worker: "test"},
			{Name: "Task 3", Worker: "test", DependsOn: []string{"Task 1"}},
		},
	}

	master := impl.NewMasterServer(10, time.Duration(0), time.Duration(0))
	_, err := master.CreateWorkflow(context.Background(), &proto.CreateWorkflowRequest{Name: "test", Tasks: workflow.Tasks})
	if err == nil {
		t.Errorf("Expected workflow creation fails because of cycle in tasks, got: %v", err)
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

func OrChannel(ch1 <-chan any, ch2 <-chan any) <-chan any {
	out := make(chan any)
	go func() {
		defer close(out)
		for {
			select {
			case v, more := <-ch1:
				if more {
					log.Printf("OrChannel Received from ch1: %v", v)
					out <- v
				} else {
					log.Printf("OrChannel ch1 closed")
					ch1 = nil
				}
			case v, more := <-ch2:
				if more {
					log.Printf("OrChannel Received from ch2: %v", v)
					out <- v
				} else {
					log.Printf("OrChannel ch2 closed")
					ch2 = nil
				}
			}
			if ch1 == nil && ch2 == nil {
				break
			}
		}
	}()
	return out
}

func TestOrChannel(t *testing.T) {
	ch1 := make(chan any)
	ch2 := make(chan any)

	go func() {
		log.Print("Sending from channel 1")
		ch1 <- "from channel 1"
		log.Print("Sent from channel 1")
		close(ch1)

	}()

	go func() {
		log.Print("Sending from channel 2")
		ch2 <- "from channel 2"
		log.Print("Sent from channel 2")
		time.Sleep(1 * time.Second)
		log.Print("Sending from channel 2")
		ch2 <- "from channel 2"
		log.Print("Sent from channel 2")
		close(ch2)
	}()

	// time.Sleep(1 * time.Second)

	or := OrChannel(ch1, ch2)
	// loop:
	// 	for {
	// 		select {
	// 		case res, more := <-or:
	// 			if !more {
	// 				break loop
	// 			}
	// 			t.Logf("Received: %v", res)
	// 		case <-time.After(time.Second):
	// 			t.Fatal("Timeout waiting for channel")
	// 		}
	// 	}

	for res := range or {
		t.Logf("Received: %v", res)
	}
	t.Log("TestOrChannel completed")
}
