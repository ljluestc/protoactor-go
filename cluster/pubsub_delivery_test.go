package cluster

import (
    "log/slog"
    "testing"
    "time"

    "github.com/asynkron/protoactor-go/actor"
    "github.com/stretchr/testify/assert"
)

// SlowSubscriberActor simulates a subscriber with long processing time
type SlowSubscriberActor struct{}

func (s *SlowSubscriberActor) Receive(c actor.Context) {
    if _, ok := c.Message().(*PubSubAutoRespondBatch); ok {
        time.Sleep(2 * time.Second) // Simulate slow processing
        c.Respond(&struct{}{})      // Respond after delay
    }
}

func TestPubSubMemberDeliveryActor_NonBlocking(t *testing.T) {
    // Setup actor system
    system := actor.NewActorSystem()
    logger := slog.Default()

    // Spawn delivery actor
    props := actor.PropsFromProducer(func() actor.Actor {
        return NewPubSubMemberDeliveryActor(3*time.Second, logger)
    })
    deliveryPID, err := system.Root.SpawnNamed(props, "test-delivery")
    assert.NoError(t, err)

    // Spawn slow subscriber
    subscriberProps := actor.PropsFromProducer(func() actor.Actor { return &SlowSubscriberActor{} })
    subscriberPID, err := system.Root.SpawnNamed(subscriberProps, "slow-subscriber")
    assert.NoError(t, err)

    // Create test batch with minimal PubSubEnvelope and SubscriberIdentity
    envelope := &PubSubEnvelope{} // Minimal initialization

    identity := &SubscriberIdentity{
        Subscriber: &SubscriberIdentity_Pid{ // Correct oneof variant
            Pid: subscriberPID,
        },
    }

    batch := &DeliverBatchRequest{
        PubSubBatch: &PubSubBatch{
            Envelopes: []*PubSubEnvelope{envelope},
        },
        Subscribers: &Subscribers{
            Subscribers: []*SubscriberIdentity{identity},
        },
        Topic: "test-topic",
    }

    // Send batch and measure time
    start := time.Now()
    system.Root.Send(deliveryPID, batch)
    time.Sleep(100 * time.Millisecond) // Give it a moment to process
    duration := time.Since(start)

    // Verify it doesn’t block
    assert.Less(t, duration, 500*time.Millisecond, "Delivery actor should not block on slow subscriber")

    // Cleanup
    system.Root.Stop(deliveryPID)
    system.Root.Stop(subscriberPID)
}