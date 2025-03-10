// cluster/pubsub_test.go
package cluster

import (
    "os"
    "testing"
    "time"
    "github.com/asynkron/protoactor-go/actor"
    "github.com/stretchr/testify/assert"
    "log/slog"
)

type SlowSubscriber struct {
    received int
}

func (s *SlowSubscriber) Receive(context actor.Context) {
    switch msg := context.Message().(type) {
    case *PubSubAutoRespondBatch:
        s.received += len(msg.Envelopes)
        time.Sleep(2 * time.Second) // Simulate slow processing
    }
}

func TestPubSubMemberDeliveryActorNonBlocking(t *testing.T) {
    system := actor.NewActorSystem()
    logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

    // Spawn PubSubMemberDeliveryActor
    deliveryProps := actor.PropsFromProducer(func() actor.Actor {
        return NewPubSubMemberDeliveryActor(5*time.Second, logger)
    })
    deliveryPID := system.Root.Spawn(deliveryProps)

    // Spawn slow subscriber
    slowSub := &SlowSubscriber{}
    slowSubProps := actor.PropsFromProducer(func() actor.Actor { return slowSub })
    slowSubPID := system.Root.Spawn(slowSubProps)

    // Send batch
    batch := &DeliverBatchRequest{
        Topic: "test",
        PubSubBatch: &Batch{
            Envelopes: []*Envelope{
                {Message: "Msg1", Topic: "test"},
                {Message: "Msg2", Topic: "test"},
            },
        },
        Subscribers: &SubscriberIdentityList{
            Subscribers: []*SubscriberIdentity{{Pid: slowSubPID}},
        },
    }
    start := time.Now()
    for i := 0; i < 3; i++ {
        system.Root.Send(deliveryPID, batch)
        time.Sleep(100 * time.Millisecond)
    }

    // Check non-blocking
    time.Sleep(1 * time.Second)
    assert.True(t, slowSub.received >= 2, "Expected at least one batch delivered")
    duration := time.Since(start)
    assert.True(t, duration < 2*time.Second, "Delivery should not block, took %v", duration)

    // Cleanup
    system.Root.Stop(deliveryPID)
}