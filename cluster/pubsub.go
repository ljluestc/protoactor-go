package cluster

import (
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/extensions"
)

const PubSubDeliveryName = "$pubsub-delivery"

var pubsubExtensionID = extensions.NextExtensionID()

type PubSub struct {
	cluster *Cluster
}

func NewPubSub(cluster *Cluster) *PubSub {
	p := &PubSub{
		cluster: cluster,
	}
	cluster.ActorSystem.Extensions.Register(p)
	return p
}

// Start the PubSubMemberDeliveryActor
func (p *PubSub) Start() {
	props := actor.PropsFromProducer(func() actor.Actor {
		return NewPubSubMemberDeliveryActor(p.cluster.Config.PubSubConfig.SubscriberTimeout, p.cluster.Logger())
	})
	_, err := p.cluster.ActorSystem.Root.SpawnNamed(props, PubSubDeliveryName)
	if err != nil {
		panic(err) // let it crash
	}
	p.cluster.Logger().Info("Started Cluster PubSub")
}

func (p *PubSub) ExtensionID() extensions.ExtensionID {
	return pubsubExtensionID
}

type PubSubConfig struct {
	// SubscriberTimeout is a timeout used when delivering a message batch to a subscriber. Default is 5s.
	//
	// This value gets rounded to seconds for optimization of cancellation token creation. Note that internally,
	// cluster request is used to deliver messages to ClusterIdentity subscribers.
	SubscriberTimeout time.Duration
}

func newPubSubConfig() *PubSubConfig {
	return &PubSubConfig{
		SubscriberTimeout: 5 * time.Second,
	}
}

// GetPubSub returns the PubSub extension from the actor system
func GetPubSub(system *actor.ActorSystem) *PubSub {
	return system.Extensions.Get(pubsubExtensionID).(*PubSub)
}

// Receive handles messages for the PubSubMemberDeliveryActor
func (p *PubSubMemberDeliveryActor) Receive(c actor.Context) {
    switch msg := c.Message().(type) {
    case *actor.Started:
        p.logger.Info("PubSubMemberDeliveryActor started")

    case *DeliverBatchRequest:
        topicBatch := &PubSubAutoRespondBatch{Envelopes: msg.PubSubBatch.Envelopes}
        siList := msg.Subscribers.Subscribers

        invalidDeliveries := make([]*SubscriberDeliveryReport, 0, len(siList))

        type futureWithIdentity struct {
            future   *actor.Future
            identity *SubscriberIdentity
        }
        futureList := make([]futureWithIdentity, 0, len(siList))
        for _, identity := range siList {
            f := p.DeliverBatch(c, topicBatch, identity)
            if f != nil {
                futureList = append(futureList, futureWithIdentity{future: f, identity: identity})
            }
        }

        for _, fWithIdentity := range futureList {
            _, err := fWithIdentity.future.Result()
            identityLog := func(err error) {
                if p.shouldThrottle() == actor.Open {
                    if fWithIdentity.identity.GetPid() != nil {
                        p.logger.Error("Pub-sub message failed to deliver to PID", slog.String("pid", fWithIdentity.identity.GetPid().String()), slog.Any("error", err))
                    } else if fWithIdentity.identity.GetClusterIdentity() != nil {
                        p.logger.Error("Pub-sub message failed to deliver to cluster identity", slog.String("cluster identity", fWithIdentity.identity.GetClusterIdentity().String()), slog.Any("error", err))
                    }
                }
            }

            status := DeliveryStatus_Delivered
            if err != nil {
                switch err {
                case actor.ErrTimeout, remote.ErrTimeout:
                    identityLog(err)
                    status = DeliveryStatus_Timeout
                case actor.ErrDeadLetter, remote.ErrDeadLetter:
                    identityLog(err)
                    status = DeliveryStatus_SubscriberNoLongerReachable
                default:
                    identityLog(err)
                    status = DeliveryStatus_OtherError
                }
            }
            if status != DeliveryStatus_Delivered {
                invalidDeliveries = append(invalidDeliveries, &SubscriberDeliveryReport{Status: status, Subscriber: fWithIdentity.identity})
            }
        }

        if len(invalidDeliveries) > 0 {
            cluster := GetCluster(c.ActorSystem())
            _, _ = cluster.Request(msg.Topic, TopicActorKind, &NotifyAboutFailingSubscribersRequest{InvalidDeliveries: invalidDeliveries})
        }

    case *Batch: // Legacy support for simpler batch type
        for _, pid := range p.getSubscribers() {
            future := c.RequestFuture(pid, msg, p.subscriberTimeout)
            go func(f *actor.Future, subscriberPID *actor.PID) {
                _, err := f.Result()
                if err != nil {
                    p.logger.Errorf("Failed to deliver legacy batch to %v: %v", subscriberPID, err)
                }
            }(future, pid)
        }

    case *AddSubscriber:
        p.addSubscriber(msg.Subscriber)
        p.logger.Infof("Added subscriber %v", msg.Subscriber)

    case *RemoveSubscriber:
        p.removeSubscriber(msg.Subscriber)
        p.logger.Infof("Removed subscriber %v", msg.Subscriber)

    default:
        p.logger.Warnf("Unknown message type: %v", msg)
    }
}

// Helper methods for legacy subscriber management
func (p *PubSubMemberDeliveryActor) getSubscribers() map[string]*actor.PID {
    // Simplified; in real Proto.Actor, this might be managed differently
    return make(map[string]*actor.PID) // Placeholder; integrate with actual subscriber list if needed
}

func (p *PubSubMemberDeliveryActor) addSubscriber(pid *actor.PID) {
    // Placeholder; integrate with actual subscriber management
}

func (p *PubSubMemberDeliveryActor) removeSubscriber(pid *actor.PID) {
    // Placeholder; integrate with actual subscriber management
}

// DeliverBatch delivers PubSubAutoRespondBatch to SubscriberIdentity
func (p *PubSubMemberDeliveryActor) DeliverBatch(c actor.Context, batch *PubSubAutoRespondBatch, s *SubscriberIdentity) *actor.Future {
    if pid := s.GetPid(); pid != nil {
        return p.DeliverToPid(c, batch, pid)
    }
    if ci := s.GetClusterIdentity(); ci != nil {
        return p.DeliverToClusterIdentity(c, batch, ci)
    }
    return nil
}

func (p *PubSubMemberDeliveryActor) DeliverToPid(c actor.Context, batch *PubSubAutoRespondBatch, pid *actor.PID) *actor.Future {
    return c.RequestFuture(pid, batch, p.subscriberTimeout)
}

func (p *PubSubMemberDeliveryActor) DeliverToClusterIdentity(c actor.Context, batch *PubSubAutoRespondBatch, ci *ClusterIdentity) *actor.Future {
    cluster := GetCluster(c.ActorSystem())
    pid := cluster.Get(ci.Identity, ci.Kind)
    return c.RequestFuture(pid, batch, p.subscriberTimeout)
}

type Batch struct {
    Envelopes []*Envelope
}

type Envelope struct {
    Message interface{}
    Topic   string
}

type SubscriberIdentityList struct {
    Subscribers []*SubscriberIdentity
}
