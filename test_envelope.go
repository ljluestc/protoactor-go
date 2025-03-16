package main

import (
    "fmt"
    "time"

    "github.com/asynkron/protoactor-go/actor"
    "github.com/asynkron/protoactor-go/persistence/protocb"
    "github.com/asynkron/protoactor-go/remote"
)

type TestMessage struct {
    Text string `protobuf:"bytes,1,opt,name=text,proto3" json:"text,omitempty"`
}

func (m *TestMessage) Reset()         { *m = TestMessage{} }
func (m *TestMessage) String() string { return fmt.Sprintf("%v", *m) }
func (m *TestMessage) ProtoMessage()  {}

func main() {
    system := actor.NewActorSystem()
    config := remote.Configure("127.0.0.1", 0)
    remoter := remote.NewRemote(system, config)
    remoter.Start()

    msg := &TestMessage{Text: "Hello World"}
    envelope := protocb.NewEnvelope(msg, "event", 1)
    unwrapped := envelope.Message()
    if unwrappedMsg, ok := unwrapped.(*TestMessage); ok {
        fmt.Println("Unwrapped Message:", unwrappedMsg.Text)
    } else {
        fmt.Println("Type Assertion Failed")
    }

    time.Sleep(1 * time.Second)
}