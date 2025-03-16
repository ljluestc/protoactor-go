package cluster

import (
    "log/slog"
    "time"

    "github.com/asynkron/protoactor-go/actor"
    "github.com/asynkron/protoactor-go/remote"
)

type PubSubMemberDeliveryActor struct {
    subscriberTimeout time.Duration
    shouldThrottle    actor.ShouldThrottle
}

func NewPubSubMemberDeliveryActor(subscriberTimeout time.Duration, logger *slog.Logger) *PubSubMemberDeliveryActor {
    return &PubSubMemberDeliveryActor{
        subscriberTimeout: subscriberTimeout,
        shouldThrottle: actor.NewThrottleWithLogger(logger, 10, time.Second, func(logger *slog.Logger, i int32) {
            logger.Warn("[PubSubMemberDeliveryActor] Throttled logs", slog.Int("count", int(i)))
        }),
    }
}

func (p *PubSubMemberDeliveryActor) Receive(c actor.Context) {
    if batch, ok := c.Message().(*DeliverBatchRequest); ok {
        topicBatch := &PubSubAutoRespondBatch{Envelopes: batch.PubSubBatch.Envelopes}
        siList := batch.Subscribers.Subscribers

        invalidDeliveries := make([]*SubscriberDeliveryReport, 0, len(siList))

        type futureWithIdentity struct {
            future   *actor.Future
            identity *SubscriberIdentity
        }
        futureList := make([]futureWithIdentity, 0, len(siList))

        // Deliver batches asynchronously
        for _, identity := range siList {
            f := p.DeliverBatch(c, topicBatch, identity)
            if f != nil {
                futureList = append(futureList, futureWithIdentity{future: f, identity: identity})
            }
        }

        // Process results asynchronously with corrected ReenterAfter
        for _, fWithIdentity := range futureList {
            c.ReenterAfter(fWithIdentity.future, func(res interface{}, err error) {
                identityLog := func(err error) {
                    if p.shouldThrottle() == actor.Open {
                        if fWithIdentity.identity.GetPid() != nil {
                            c.Logger().Error("Pub-sub message failed to deliver to PID",
                                slog.String("pid", fWithIdentity.identity.GetPid().String()),
                                slog.Any("error", err))
                        } else if fWithIdentity.identity.GetClusterIdentity() != nil {
                            c.Logger().Error("Pub-sub message failed to deliver to cluster identity",
                                slog.String("cluster identity", fWithIdentity.identity.GetClusterIdentity().String()),
                                slog.Any("error", err))
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
                    invalidDeliveries = append(invalidDeliveries,
                        &SubscriberDeliveryReport{Status: status, Subscriber: fWithIdentity.identity})
                }
            })
        }

        // Notify about failing subscribers (non-blocking)
        if len(invalidDeliveries) > 0 {
            cluster := GetCluster(c.ActorSystem())
            _, _ = cluster.Request(batch.Topic, TopicActorKind, &NotifyAboutFailingSubscribersRequest{InvalidDeliveries: invalidDeliveries})
        }
    }
}

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
    future := actor.NewFuture(c.ActorSystem(), p.subscriberTimeout)
    c.Send(pid, batch)
    return future
}

func (p *PubSubMemberDeliveryActor) DeliverToClusterIdentity(c actor.Context, batch *PubSubAutoRespondBatch, ci *ClusterIdentity) *actor.Future {
    cluster := GetCluster(c.ActorSystem())
    pid := cluster.Get(ci.Identity, ci.Kind)
    future := actor.NewFuture(c.ActorSystem(), p.subscriberTimeout)
    c.Send(pid, batch)
    return future // Fixed typo from 'futurer-go$' to 'future'
}