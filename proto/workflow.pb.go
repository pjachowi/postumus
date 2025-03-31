// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.5
// 	protoc        v3.21.12
// source: workflow.proto

package proto

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
	unsafe "unsafe"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type Task_Status int32

const (
	Task_UNKNOWN   Task_Status = 0
	Task_PENDING   Task_Status = 1
	Task_RUNNING   Task_Status = 2
	Task_COMPLETED Task_Status = 3
	Task_FAILED    Task_Status = 4
)

// Enum value maps for Task_Status.
var (
	Task_Status_name = map[int32]string{
		0: "UNKNOWN",
		1: "PENDING",
		2: "RUNNING",
		3: "COMPLETED",
		4: "FAILED",
	}
	Task_Status_value = map[string]int32{
		"UNKNOWN":   0,
		"PENDING":   1,
		"RUNNING":   2,
		"COMPLETED": 3,
		"FAILED":    4,
	}
)

func (x Task_Status) Enum() *Task_Status {
	p := new(Task_Status)
	*p = x
	return p
}

func (x Task_Status) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (Task_Status) Descriptor() protoreflect.EnumDescriptor {
	return file_workflow_proto_enumTypes[0].Descriptor()
}

func (Task_Status) Type() protoreflect.EnumType {
	return &file_workflow_proto_enumTypes[0]
}

func (x Task_Status) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use Task_Status.Descriptor instead.
func (Task_Status) EnumDescriptor() ([]byte, []int) {
	return file_workflow_proto_rawDescGZIP(), []int{0, 0}
}

type Workflow_Status int32

const (
	Workflow_UNKNOWN   Workflow_Status = 0
	Workflow_PENDING   Workflow_Status = 1
	Workflow_RUNNING   Workflow_Status = 2
	Workflow_COMPLETED Workflow_Status = 3
)

// Enum value maps for Workflow_Status.
var (
	Workflow_Status_name = map[int32]string{
		0: "UNKNOWN",
		1: "PENDING",
		2: "RUNNING",
		3: "COMPLETED",
	}
	Workflow_Status_value = map[string]int32{
		"UNKNOWN":   0,
		"PENDING":   1,
		"RUNNING":   2,
		"COMPLETED": 3,
	}
)

func (x Workflow_Status) Enum() *Workflow_Status {
	p := new(Workflow_Status)
	*p = x
	return p
}

func (x Workflow_Status) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (Workflow_Status) Descriptor() protoreflect.EnumDescriptor {
	return file_workflow_proto_enumTypes[1].Descriptor()
}

func (Workflow_Status) Type() protoreflect.EnumType {
	return &file_workflow_proto_enumTypes[1]
}

func (x Workflow_Status) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use Workflow_Status.Descriptor instead.
func (Workflow_Status) EnumDescriptor() ([]byte, []int) {
	return file_workflow_proto_rawDescGZIP(), []int{2, 0}
}

type Task struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Id            string                 `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Name          string                 `protobuf:"bytes,2,opt,name=name,proto3" json:"name,omitempty"`
	Worker        string                 `protobuf:"bytes,3,opt,name=worker,proto3" json:"worker,omitempty"`
	Definition    string                 `protobuf:"bytes,4,opt,name=definition,proto3" json:"definition,omitempty"`
	Status        Task_Status            `protobuf:"varint,5,opt,name=status,proto3,enum=Task_Status" json:"status,omitempty"`
	MaxAttempts   int32                  `protobuf:"varint,6,opt,name=max_attempts,json=maxAttempts,proto3" json:"max_attempts,omitempty"`
	Attempts      int32                  `protobuf:"varint,7,opt,name=attempts,proto3" json:"attempts,omitempty"`
	WorkflowId    string                 `protobuf:"bytes,8,opt,name=workflow_id,json=workflowId,proto3" json:"workflow_id,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Task) Reset() {
	*x = Task{}
	mi := &file_workflow_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Task) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Task) ProtoMessage() {}

func (x *Task) ProtoReflect() protoreflect.Message {
	mi := &file_workflow_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Task.ProtoReflect.Descriptor instead.
func (*Task) Descriptor() ([]byte, []int) {
	return file_workflow_proto_rawDescGZIP(), []int{0}
}

func (x *Task) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *Task) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *Task) GetWorker() string {
	if x != nil {
		return x.Worker
	}
	return ""
}

func (x *Task) GetDefinition() string {
	if x != nil {
		return x.Definition
	}
	return ""
}

func (x *Task) GetStatus() Task_Status {
	if x != nil {
		return x.Status
	}
	return Task_UNKNOWN
}

func (x *Task) GetMaxAttempts() int32 {
	if x != nil {
		return x.MaxAttempts
	}
	return 0
}

func (x *Task) GetAttempts() int32 {
	if x != nil {
		return x.Attempts
	}
	return 0
}

func (x *Task) GetWorkflowId() string {
	if x != nil {
		return x.WorkflowId
	}
	return ""
}

type Notask struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Notask) Reset() {
	*x = Notask{}
	mi := &file_workflow_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Notask) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Notask) ProtoMessage() {}

func (x *Notask) ProtoReflect() protoreflect.Message {
	mi := &file_workflow_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Notask.ProtoReflect.Descriptor instead.
func (*Notask) Descriptor() ([]byte, []int) {
	return file_workflow_proto_rawDescGZIP(), []int{1}
}

type Workflow struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Id            string                 `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Name          string                 `protobuf:"bytes,2,opt,name=name,proto3" json:"name,omitempty"`
	Status        Workflow_Status        `protobuf:"varint,4,opt,name=status,proto3,enum=Workflow_Status" json:"status,omitempty"`
	Tasks         []*Task                `protobuf:"bytes,5,rep,name=tasks,proto3" json:"tasks,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Workflow) Reset() {
	*x = Workflow{}
	mi := &file_workflow_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Workflow) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Workflow) ProtoMessage() {}

func (x *Workflow) ProtoReflect() protoreflect.Message {
	mi := &file_workflow_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Workflow.ProtoReflect.Descriptor instead.
func (*Workflow) Descriptor() ([]byte, []int) {
	return file_workflow_proto_rawDescGZIP(), []int{2}
}

func (x *Workflow) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *Workflow) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *Workflow) GetStatus() Workflow_Status {
	if x != nil {
		return x.Status
	}
	return Workflow_UNKNOWN
}

func (x *Workflow) GetTasks() []*Task {
	if x != nil {
		return x.Tasks
	}
	return nil
}

type CreateWorkflowRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Name          string                 `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Tasks         []*Task                `protobuf:"bytes,2,rep,name=tasks,proto3" json:"tasks,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *CreateWorkflowRequest) Reset() {
	*x = CreateWorkflowRequest{}
	mi := &file_workflow_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *CreateWorkflowRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CreateWorkflowRequest) ProtoMessage() {}

func (x *CreateWorkflowRequest) ProtoReflect() protoreflect.Message {
	mi := &file_workflow_proto_msgTypes[3]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CreateWorkflowRequest.ProtoReflect.Descriptor instead.
func (*CreateWorkflowRequest) Descriptor() ([]byte, []int) {
	return file_workflow_proto_rawDescGZIP(), []int{3}
}

func (x *CreateWorkflowRequest) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *CreateWorkflowRequest) GetTasks() []*Task {
	if x != nil {
		return x.Tasks
	}
	return nil
}

type CreateWorkflowResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Id            string                 `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *CreateWorkflowResponse) Reset() {
	*x = CreateWorkflowResponse{}
	mi := &file_workflow_proto_msgTypes[4]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *CreateWorkflowResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CreateWorkflowResponse) ProtoMessage() {}

func (x *CreateWorkflowResponse) ProtoReflect() protoreflect.Message {
	mi := &file_workflow_proto_msgTypes[4]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CreateWorkflowResponse.ProtoReflect.Descriptor instead.
func (*CreateWorkflowResponse) Descriptor() ([]byte, []int) {
	return file_workflow_proto_rawDescGZIP(), []int{4}
}

func (x *CreateWorkflowResponse) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

type GetWorkflowRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Id            string                 `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetWorkflowRequest) Reset() {
	*x = GetWorkflowRequest{}
	mi := &file_workflow_proto_msgTypes[5]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetWorkflowRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetWorkflowRequest) ProtoMessage() {}

func (x *GetWorkflowRequest) ProtoReflect() protoreflect.Message {
	mi := &file_workflow_proto_msgTypes[5]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetWorkflowRequest.ProtoReflect.Descriptor instead.
func (*GetWorkflowRequest) Descriptor() ([]byte, []int) {
	return file_workflow_proto_rawDescGZIP(), []int{5}
}

func (x *GetWorkflowRequest) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

type GetWorkflowResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Workflow      *Workflow              `protobuf:"bytes,1,opt,name=workflow,proto3" json:"workflow,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetWorkflowResponse) Reset() {
	*x = GetWorkflowResponse{}
	mi := &file_workflow_proto_msgTypes[6]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetWorkflowResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetWorkflowResponse) ProtoMessage() {}

func (x *GetWorkflowResponse) ProtoReflect() protoreflect.Message {
	mi := &file_workflow_proto_msgTypes[6]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetWorkflowResponse.ProtoReflect.Descriptor instead.
func (*GetWorkflowResponse) Descriptor() ([]byte, []int) {
	return file_workflow_proto_rawDescGZIP(), []int{6}
}

func (x *GetWorkflowResponse) GetWorkflow() *Workflow {
	if x != nil {
		return x.Workflow
	}
	return nil
}

type GetWorkflowIdsRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetWorkflowIdsRequest) Reset() {
	*x = GetWorkflowIdsRequest{}
	mi := &file_workflow_proto_msgTypes[7]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetWorkflowIdsRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetWorkflowIdsRequest) ProtoMessage() {}

func (x *GetWorkflowIdsRequest) ProtoReflect() protoreflect.Message {
	mi := &file_workflow_proto_msgTypes[7]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetWorkflowIdsRequest.ProtoReflect.Descriptor instead.
func (*GetWorkflowIdsRequest) Descriptor() ([]byte, []int) {
	return file_workflow_proto_rawDescGZIP(), []int{7}
}

type GetWorkflowIdsResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Ids           []string               `protobuf:"bytes,1,rep,name=ids,proto3" json:"ids,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetWorkflowIdsResponse) Reset() {
	*x = GetWorkflowIdsResponse{}
	mi := &file_workflow_proto_msgTypes[8]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetWorkflowIdsResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetWorkflowIdsResponse) ProtoMessage() {}

func (x *GetWorkflowIdsResponse) ProtoReflect() protoreflect.Message {
	mi := &file_workflow_proto_msgTypes[8]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetWorkflowIdsResponse.ProtoReflect.Descriptor instead.
func (*GetWorkflowIdsResponse) Descriptor() ([]byte, []int) {
	return file_workflow_proto_rawDescGZIP(), []int{8}
}

func (x *GetWorkflowIdsResponse) GetIds() []string {
	if x != nil {
		return x.Ids
	}
	return nil
}

type GetTaskRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Worker        string                 `protobuf:"bytes,1,opt,name=worker,proto3" json:"worker,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetTaskRequest) Reset() {
	*x = GetTaskRequest{}
	mi := &file_workflow_proto_msgTypes[9]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetTaskRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetTaskRequest) ProtoMessage() {}

func (x *GetTaskRequest) ProtoReflect() protoreflect.Message {
	mi := &file_workflow_proto_msgTypes[9]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetTaskRequest.ProtoReflect.Descriptor instead.
func (*GetTaskRequest) Descriptor() ([]byte, []int) {
	return file_workflow_proto_rawDescGZIP(), []int{9}
}

func (x *GetTaskRequest) GetWorker() string {
	if x != nil {
		return x.Worker
	}
	return ""
}

type GetTaskResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Types that are valid to be assigned to Response:
	//
	//	*GetTaskResponse_Task
	//	*GetTaskResponse_Notask
	Response      isGetTaskResponse_Response `protobuf_oneof:"response"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetTaskResponse) Reset() {
	*x = GetTaskResponse{}
	mi := &file_workflow_proto_msgTypes[10]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetTaskResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetTaskResponse) ProtoMessage() {}

func (x *GetTaskResponse) ProtoReflect() protoreflect.Message {
	mi := &file_workflow_proto_msgTypes[10]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetTaskResponse.ProtoReflect.Descriptor instead.
func (*GetTaskResponse) Descriptor() ([]byte, []int) {
	return file_workflow_proto_rawDescGZIP(), []int{10}
}

func (x *GetTaskResponse) GetResponse() isGetTaskResponse_Response {
	if x != nil {
		return x.Response
	}
	return nil
}

func (x *GetTaskResponse) GetTask() *Task {
	if x != nil {
		if x, ok := x.Response.(*GetTaskResponse_Task); ok {
			return x.Task
		}
	}
	return nil
}

func (x *GetTaskResponse) GetNotask() *Notask {
	if x != nil {
		if x, ok := x.Response.(*GetTaskResponse_Notask); ok {
			return x.Notask
		}
	}
	return nil
}

type isGetTaskResponse_Response interface {
	isGetTaskResponse_Response()
}

type GetTaskResponse_Task struct {
	Task *Task `protobuf:"bytes,1,opt,name=task,proto3,oneof"`
}

type GetTaskResponse_Notask struct {
	Notask *Notask `protobuf:"bytes,2,opt,name=notask,proto3,oneof"`
}

func (*GetTaskResponse_Task) isGetTaskResponse_Response() {}

func (*GetTaskResponse_Notask) isGetTaskResponse_Response() {}

type ReportTaskResultRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Task          *Task                  `protobuf:"bytes,1,opt,name=task,proto3" json:"task,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ReportTaskResultRequest) Reset() {
	*x = ReportTaskResultRequest{}
	mi := &file_workflow_proto_msgTypes[11]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ReportTaskResultRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ReportTaskResultRequest) ProtoMessage() {}

func (x *ReportTaskResultRequest) ProtoReflect() protoreflect.Message {
	mi := &file_workflow_proto_msgTypes[11]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ReportTaskResultRequest.ProtoReflect.Descriptor instead.
func (*ReportTaskResultRequest) Descriptor() ([]byte, []int) {
	return file_workflow_proto_rawDescGZIP(), []int{11}
}

func (x *ReportTaskResultRequest) GetTask() *Task {
	if x != nil {
		return x.Task
	}
	return nil
}

type ReportTaskResultResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ReportTaskResultResponse) Reset() {
	*x = ReportTaskResultResponse{}
	mi := &file_workflow_proto_msgTypes[12]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ReportTaskResultResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ReportTaskResultResponse) ProtoMessage() {}

func (x *ReportTaskResultResponse) ProtoReflect() protoreflect.Message {
	mi := &file_workflow_proto_msgTypes[12]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ReportTaskResultResponse.ProtoReflect.Descriptor instead.
func (*ReportTaskResultResponse) Descriptor() ([]byte, []int) {
	return file_workflow_proto_rawDescGZIP(), []int{12}
}

var File_workflow_proto protoreflect.FileDescriptor

var file_workflow_proto_rawDesc = string([]byte{
	0x0a, 0x0e, 0x77, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x22, 0xb4, 0x02, 0x0a, 0x04, 0x54, 0x61, 0x73, 0x6b, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02, 0x69, 0x64, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d,
	0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x16, 0x0a,
	0x06, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x77,
	0x6f, 0x72, 0x6b, 0x65, 0x72, 0x12, 0x1e, 0x0a, 0x0a, 0x64, 0x65, 0x66, 0x69, 0x6e, 0x69, 0x74,
	0x69, 0x6f, 0x6e, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0a, 0x64, 0x65, 0x66, 0x69, 0x6e,
	0x69, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x24, 0x0a, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x18,
	0x05, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x0c, 0x2e, 0x54, 0x61, 0x73, 0x6b, 0x2e, 0x53, 0x74, 0x61,
	0x74, 0x75, 0x73, 0x52, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x12, 0x21, 0x0a, 0x0c, 0x6d,
	0x61, 0x78, 0x5f, 0x61, 0x74, 0x74, 0x65, 0x6d, 0x70, 0x74, 0x73, 0x18, 0x06, 0x20, 0x01, 0x28,
	0x05, 0x52, 0x0b, 0x6d, 0x61, 0x78, 0x41, 0x74, 0x74, 0x65, 0x6d, 0x70, 0x74, 0x73, 0x12, 0x1a,
	0x0a, 0x08, 0x61, 0x74, 0x74, 0x65, 0x6d, 0x70, 0x74, 0x73, 0x18, 0x07, 0x20, 0x01, 0x28, 0x05,
	0x52, 0x08, 0x61, 0x74, 0x74, 0x65, 0x6d, 0x70, 0x74, 0x73, 0x12, 0x1f, 0x0a, 0x0b, 0x77, 0x6f,
	0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x5f, 0x69, 0x64, 0x18, 0x08, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x0a, 0x77, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x49, 0x64, 0x22, 0x4a, 0x0a, 0x06, 0x53,
	0x74, 0x61, 0x74, 0x75, 0x73, 0x12, 0x0b, 0x0a, 0x07, 0x55, 0x4e, 0x4b, 0x4e, 0x4f, 0x57, 0x4e,
	0x10, 0x00, 0x12, 0x0b, 0x0a, 0x07, 0x50, 0x45, 0x4e, 0x44, 0x49, 0x4e, 0x47, 0x10, 0x01, 0x12,
	0x0b, 0x0a, 0x07, 0x52, 0x55, 0x4e, 0x4e, 0x49, 0x4e, 0x47, 0x10, 0x02, 0x12, 0x0d, 0x0a, 0x09,
	0x43, 0x4f, 0x4d, 0x50, 0x4c, 0x45, 0x54, 0x45, 0x44, 0x10, 0x03, 0x12, 0x0a, 0x0a, 0x06, 0x46,
	0x41, 0x49, 0x4c, 0x45, 0x44, 0x10, 0x04, 0x22, 0x08, 0x0a, 0x06, 0x4e, 0x6f, 0x74, 0x61, 0x73,
	0x6b, 0x22, 0xb5, 0x01, 0x0a, 0x08, 0x57, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x12, 0x0e,
	0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02, 0x69, 0x64, 0x12, 0x12,
	0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61,
	0x6d, 0x65, 0x12, 0x28, 0x0a, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x18, 0x04, 0x20, 0x01,
	0x28, 0x0e, 0x32, 0x10, 0x2e, 0x57, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x2e, 0x53, 0x74,
	0x61, 0x74, 0x75, 0x73, 0x52, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x12, 0x1b, 0x0a, 0x05,
	0x74, 0x61, 0x73, 0x6b, 0x73, 0x18, 0x05, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x05, 0x2e, 0x54, 0x61,
	0x73, 0x6b, 0x52, 0x05, 0x74, 0x61, 0x73, 0x6b, 0x73, 0x22, 0x3e, 0x0a, 0x06, 0x53, 0x74, 0x61,
	0x74, 0x75, 0x73, 0x12, 0x0b, 0x0a, 0x07, 0x55, 0x4e, 0x4b, 0x4e, 0x4f, 0x57, 0x4e, 0x10, 0x00,
	0x12, 0x0b, 0x0a, 0x07, 0x50, 0x45, 0x4e, 0x44, 0x49, 0x4e, 0x47, 0x10, 0x01, 0x12, 0x0b, 0x0a,
	0x07, 0x52, 0x55, 0x4e, 0x4e, 0x49, 0x4e, 0x47, 0x10, 0x02, 0x12, 0x0d, 0x0a, 0x09, 0x43, 0x4f,
	0x4d, 0x50, 0x4c, 0x45, 0x54, 0x45, 0x44, 0x10, 0x03, 0x22, 0x48, 0x0a, 0x15, 0x43, 0x72, 0x65,
	0x61, 0x74, 0x65, 0x57, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x1b, 0x0a, 0x05, 0x74, 0x61, 0x73, 0x6b, 0x73, 0x18,
	0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x05, 0x2e, 0x54, 0x61, 0x73, 0x6b, 0x52, 0x05, 0x74, 0x61,
	0x73, 0x6b, 0x73, 0x22, 0x28, 0x0a, 0x16, 0x43, 0x72, 0x65, 0x61, 0x74, 0x65, 0x57, 0x6f, 0x72,
	0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x0e, 0x0a,
	0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02, 0x69, 0x64, 0x22, 0x24, 0x0a,
	0x12, 0x47, 0x65, 0x74, 0x57, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x02, 0x69, 0x64, 0x22, 0x3c, 0x0a, 0x13, 0x47, 0x65, 0x74, 0x57, 0x6f, 0x72, 0x6b, 0x66, 0x6c,
	0x6f, 0x77, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x25, 0x0a, 0x08, 0x77, 0x6f,
	0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x09, 0x2e, 0x57,
	0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x52, 0x08, 0x77, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f,
	0x77, 0x22, 0x17, 0x0a, 0x15, 0x47, 0x65, 0x74, 0x57, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77,
	0x49, 0x64, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x22, 0x2a, 0x0a, 0x16, 0x47, 0x65,
	0x74, 0x57, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x49, 0x64, 0x73, 0x52, 0x65, 0x73, 0x70,
	0x6f, 0x6e, 0x73, 0x65, 0x12, 0x10, 0x0a, 0x03, 0x69, 0x64, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28,
	0x09, 0x52, 0x03, 0x69, 0x64, 0x73, 0x22, 0x28, 0x0a, 0x0e, 0x47, 0x65, 0x74, 0x54, 0x61, 0x73,
	0x6b, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x16, 0x0a, 0x06, 0x77, 0x6f, 0x72, 0x6b,
	0x65, 0x72, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72,
	0x22, 0x5d, 0x0a, 0x0f, 0x47, 0x65, 0x74, 0x54, 0x61, 0x73, 0x6b, 0x52, 0x65, 0x73, 0x70, 0x6f,
	0x6e, 0x73, 0x65, 0x12, 0x1b, 0x0a, 0x04, 0x74, 0x61, 0x73, 0x6b, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x0b, 0x32, 0x05, 0x2e, 0x54, 0x61, 0x73, 0x6b, 0x48, 0x00, 0x52, 0x04, 0x74, 0x61, 0x73, 0x6b,
	0x12, 0x21, 0x0a, 0x06, 0x6e, 0x6f, 0x74, 0x61, 0x73, 0x6b, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b,
	0x32, 0x07, 0x2e, 0x4e, 0x6f, 0x74, 0x61, 0x73, 0x6b, 0x48, 0x00, 0x52, 0x06, 0x6e, 0x6f, 0x74,
	0x61, 0x73, 0x6b, 0x42, 0x0a, 0x0a, 0x08, 0x72, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22,
	0x34, 0x0a, 0x17, 0x52, 0x65, 0x70, 0x6f, 0x72, 0x74, 0x54, 0x61, 0x73, 0x6b, 0x52, 0x65, 0x73,
	0x75, 0x6c, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x19, 0x0a, 0x04, 0x74, 0x61,
	0x73, 0x6b, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x05, 0x2e, 0x54, 0x61, 0x73, 0x6b, 0x52,
	0x04, 0x74, 0x61, 0x73, 0x6b, 0x22, 0x1a, 0x0a, 0x18, 0x52, 0x65, 0x70, 0x6f, 0x72, 0x74, 0x54,
	0x61, 0x73, 0x6b, 0x52, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73,
	0x65, 0x32, 0xbf, 0x02, 0x0a, 0x06, 0x4d, 0x61, 0x73, 0x74, 0x65, 0x72, 0x12, 0x41, 0x0a, 0x0e,
	0x43, 0x72, 0x65, 0x61, 0x74, 0x65, 0x57, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x12, 0x16,
	0x2e, 0x43, 0x72, 0x65, 0x61, 0x74, 0x65, 0x57, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x17, 0x2e, 0x43, 0x72, 0x65, 0x61, 0x74, 0x65, 0x57,
	0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12,
	0x38, 0x0a, 0x0b, 0x47, 0x65, 0x74, 0x57, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x12, 0x13,
	0x2e, 0x47, 0x65, 0x74, 0x57, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x1a, 0x14, 0x2e, 0x47, 0x65, 0x74, 0x57, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f,
	0x77, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x41, 0x0a, 0x0e, 0x47, 0x65, 0x74,
	0x57, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x49, 0x64, 0x73, 0x12, 0x16, 0x2e, 0x47, 0x65,
	0x74, 0x57, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x49, 0x64, 0x73, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x1a, 0x17, 0x2e, 0x47, 0x65, 0x74, 0x57, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f,
	0x77, 0x49, 0x64, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x2c, 0x0a, 0x07,
	0x47, 0x65, 0x74, 0x54, 0x61, 0x73, 0x6b, 0x12, 0x0f, 0x2e, 0x47, 0x65, 0x74, 0x54, 0x61, 0x73,
	0x6b, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x10, 0x2e, 0x47, 0x65, 0x74, 0x54, 0x61,
	0x73, 0x6b, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x47, 0x0a, 0x10, 0x52, 0x65,
	0x70, 0x6f, 0x72, 0x74, 0x54, 0x61, 0x73, 0x6b, 0x52, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x12, 0x18,
	0x2e, 0x52, 0x65, 0x70, 0x6f, 0x72, 0x74, 0x54, 0x61, 0x73, 0x6b, 0x52, 0x65, 0x73, 0x75, 0x6c,
	0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x19, 0x2e, 0x52, 0x65, 0x70, 0x6f, 0x72,
	0x74, 0x54, 0x61, 0x73, 0x6b, 0x52, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f,
	0x6e, 0x73, 0x65, 0x32, 0x36, 0x0a, 0x06, 0x57, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x12, 0x2c, 0x0a,
	0x07, 0x47, 0x65, 0x74, 0x54, 0x61, 0x73, 0x6b, 0x12, 0x0f, 0x2e, 0x47, 0x65, 0x74, 0x54, 0x61,
	0x73, 0x6b, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x10, 0x2e, 0x47, 0x65, 0x74, 0x54,
	0x61, 0x73, 0x6b, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x42, 0x17, 0x5a, 0x15, 0x66,
	0x6f, 0x6f, 0x62, 0x61, 0x72, 0x2f, 0x70, 0x6f, 0x73, 0x74, 0x75, 0x6d, 0x75, 0x73, 0x2f, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
})

var (
	file_workflow_proto_rawDescOnce sync.Once
	file_workflow_proto_rawDescData []byte
)

func file_workflow_proto_rawDescGZIP() []byte {
	file_workflow_proto_rawDescOnce.Do(func() {
		file_workflow_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_workflow_proto_rawDesc), len(file_workflow_proto_rawDesc)))
	})
	return file_workflow_proto_rawDescData
}

var file_workflow_proto_enumTypes = make([]protoimpl.EnumInfo, 2)
var file_workflow_proto_msgTypes = make([]protoimpl.MessageInfo, 13)
var file_workflow_proto_goTypes = []any{
	(Task_Status)(0),                 // 0: Task.Status
	(Workflow_Status)(0),             // 1: Workflow.Status
	(*Task)(nil),                     // 2: Task
	(*Notask)(nil),                   // 3: Notask
	(*Workflow)(nil),                 // 4: Workflow
	(*CreateWorkflowRequest)(nil),    // 5: CreateWorkflowRequest
	(*CreateWorkflowResponse)(nil),   // 6: CreateWorkflowResponse
	(*GetWorkflowRequest)(nil),       // 7: GetWorkflowRequest
	(*GetWorkflowResponse)(nil),      // 8: GetWorkflowResponse
	(*GetWorkflowIdsRequest)(nil),    // 9: GetWorkflowIdsRequest
	(*GetWorkflowIdsResponse)(nil),   // 10: GetWorkflowIdsResponse
	(*GetTaskRequest)(nil),           // 11: GetTaskRequest
	(*GetTaskResponse)(nil),          // 12: GetTaskResponse
	(*ReportTaskResultRequest)(nil),  // 13: ReportTaskResultRequest
	(*ReportTaskResultResponse)(nil), // 14: ReportTaskResultResponse
}
var file_workflow_proto_depIdxs = []int32{
	0,  // 0: Task.status:type_name -> Task.Status
	1,  // 1: Workflow.status:type_name -> Workflow.Status
	2,  // 2: Workflow.tasks:type_name -> Task
	2,  // 3: CreateWorkflowRequest.tasks:type_name -> Task
	4,  // 4: GetWorkflowResponse.workflow:type_name -> Workflow
	2,  // 5: GetTaskResponse.task:type_name -> Task
	3,  // 6: GetTaskResponse.notask:type_name -> Notask
	2,  // 7: ReportTaskResultRequest.task:type_name -> Task
	5,  // 8: Master.CreateWorkflow:input_type -> CreateWorkflowRequest
	7,  // 9: Master.GetWorkflow:input_type -> GetWorkflowRequest
	9,  // 10: Master.GetWorkflowIds:input_type -> GetWorkflowIdsRequest
	11, // 11: Master.GetTask:input_type -> GetTaskRequest
	13, // 12: Master.ReportTaskResult:input_type -> ReportTaskResultRequest
	11, // 13: Worker.GetTask:input_type -> GetTaskRequest
	6,  // 14: Master.CreateWorkflow:output_type -> CreateWorkflowResponse
	8,  // 15: Master.GetWorkflow:output_type -> GetWorkflowResponse
	10, // 16: Master.GetWorkflowIds:output_type -> GetWorkflowIdsResponse
	12, // 17: Master.GetTask:output_type -> GetTaskResponse
	14, // 18: Master.ReportTaskResult:output_type -> ReportTaskResultResponse
	12, // 19: Worker.GetTask:output_type -> GetTaskResponse
	14, // [14:20] is the sub-list for method output_type
	8,  // [8:14] is the sub-list for method input_type
	8,  // [8:8] is the sub-list for extension type_name
	8,  // [8:8] is the sub-list for extension extendee
	0,  // [0:8] is the sub-list for field type_name
}

func init() { file_workflow_proto_init() }
func file_workflow_proto_init() {
	if File_workflow_proto != nil {
		return
	}
	file_workflow_proto_msgTypes[10].OneofWrappers = []any{
		(*GetTaskResponse_Task)(nil),
		(*GetTaskResponse_Notask)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_workflow_proto_rawDesc), len(file_workflow_proto_rawDesc)),
			NumEnums:      2,
			NumMessages:   13,
			NumExtensions: 0,
			NumServices:   2,
		},
		GoTypes:           file_workflow_proto_goTypes,
		DependencyIndexes: file_workflow_proto_depIdxs,
		EnumInfos:         file_workflow_proto_enumTypes,
		MessageInfos:      file_workflow_proto_msgTypes,
	}.Build()
	File_workflow_proto = out.File
	file_workflow_proto_goTypes = nil
	file_workflow_proto_depIdxs = nil
}
