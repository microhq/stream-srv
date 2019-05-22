package handler

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/micro/go-log"

	"github.com/microhq/stream-srv/mux"
	pb "github.com/microhq/stream-srv/proto/stream"
	"github.com/microhq/stream-srv/sub"
)

// Dispatcher dispatches new messages to all subscribers
type Dispatcher struct {
	// s is a subscription to dispatch streams to
	s *sub.Subscription
}

// Dispatch dispatches the message to all subscribers in subscription s
func (d *Dispatcher) Dispatch(ch <-chan *pb.Msg, done chan struct{}, wg *sync.WaitGroup) {
	id := d.s.GetID()
	defer wg.Done()
	for {
		select {
		case <-done:
			log.Logf("Stopping dispatcher for stream: %d", id)
			return
		case msg := <-ch:
			log.Logf("Dispatching message to subscribers on stream: %d", id)
			for _, sub := range d.s.GetSubs().AsList() {
				if err := sub.GetStream().Send(msg); err != nil {
					// send the error down subscriber error channel
					sub.GetErrChan() <- err
				}
			}
		}
	}
}

// Stream is a data stream
type Stream struct {
	// Mux maps stream ids to subscribers to allow stream multiplexing
	Mux *mux.Mux
	// done channel notifies running goroutines to finish their tasks
	done chan struct{}
	// wg tracks running goroutines
	wg *sync.WaitGroup
}

func NewStream() (*Stream, error) {
	mux, err := mux.New()
	if err != nil {
		return nil, err
	}

	done := make(chan struct{})

	return &Stream{
		Mux:  mux,
		done: done,
		wg:   new(sync.WaitGroup),
	}, nil
}

func (s *Stream) Create(ctx context.Context, req *pb.CreateRequest, resp *pb.CreateResponse) error {
	log.Logf("Received Stream.Create request with id: %d", req.Id)

	if err := s.Mux.Add(req.Id); err != nil {
		return fmt.Errorf("Unable to create new stream: %s", err)
	}

	// retrieves stream subscription
	sub, err := s.Mux.GetSubscription(req.Id)
	if err != nil {
		return fmt.Errorf("Unable to retrieve subscription: %s", err)
	}

	ch, err := s.Mux.GetChan(req.Id)
	if err != nil {
		return fmt.Errorf("Unable to craete dispatch channel: %s", err)
	}

	// creates new stream dispatcher
	d := &Dispatcher{s: sub}
	s.wg.Add(1)
	go d.Dispatch(ch, s.done, s.wg)

	return nil
}

func (s *Stream) Publish(ctx context.Context, stream pb.Stream_PublishStream) error {
	var id string
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			log.Log("Stream publisher disconnected")
			break
		}

		if err != nil {
			log.Logf("Error: %v", err)
			continue
		}

		id = msg.Id
		log.Logf("Received msg for stream: %d", id)

		ch, err := s.Mux.GetChan(id)
		if err != nil {
			log.Logf("Error getting message channel: %v", err)
		} else {
			go func(msg *pb.Msg) {
				ch <- msg
			}(msg)
		}
	}

	// remove stream from Mux
	return s.Mux.Remove(id)
}

func (s *Stream) Subscribe(ctx context.Context, req *pb.SubscribeRequest, stream pb.Stream_SubscribeStream) error {
	log.Logf("Received Stream.Subscribe request for stream: %d", req.Id)

	streamId := req.Id
	errCount := 0

	sub, err := sub.NewSubscriber(stream)
	if err != nil {
		return fmt.Errorf("Failed to create subscriber: %s", err)
	}

	if err := s.Mux.AddSub(streamId, sub); err != nil {
		return fmt.Errorf("Failed to subscribe %v to stream %s", sub.GetID(), streamId)
	}

	for {
		select {
		case <-s.done:
			log.Logf("Stopping subscriber of stream: %d", streamId)
			if err := stream.Close(); err != nil {
				log.Logf("Error closing subscription stream %d: %s", streamId, err)
			}
			return s.Mux.RemSub(streamId, sub)
		case err := <-sub.GetErrChan():
			// TODO: handle this better: might want to count and remove subscriber
			if err != nil {
				log.Logf("Error receiving message on stream %d: %s", streamId, err)
				errCount++
			}
			if errCount > 5 {
				// NOTE: this is an arbitrary selected value
				log.Logf("Error threshold reached for %s on stream: %d", sub.ID, streamId)
				return s.Mux.RemSub(streamId, sub)
			}
		default:
			// do nothing here
		}
	}

	return nil
}
