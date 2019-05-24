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

// Stream is a data stream
type Stream struct {
	// Mux maps streams to data dispatchers
	Mux *mux.Mux
	// done notifies Stream  to stop
	done chan struct{}
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
	}, nil
}

// Create creates new data stream. It creates new stream dispatcher.
// It returns error if the requested stream id has already been registered.
func (s *Stream) Create(ctx context.Context, req *pb.CreateRequest, resp *pb.CreateResponse) error {
	log.Logf("Received Stream.Create request with id: %s", req.Id)

	// sdd new stream to stream multiplexer
	if err := s.Mux.AddStream(req.Id, 100); err != nil {
		return fmt.Errorf("Unable to create new stream: %s", err)
	}

	return nil
}

// Publish publishes data on stream
func (s *Stream) Publish(ctx context.Context, stream pb.Stream_PublishStream) error {
	var id string
	errCount := 0

	// track all goroutine spun out of this
	wg := &sync.WaitGroup{}

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			log.Logf("Stream publisher disconnected")
			break
		}

		if err != nil {
			log.Logf("Error publishing on stream: %v", err)
			errCount++
			continue
		}

		if errCount > 5 {
			log.Logf("Error threshold reached for stream")
			break
		}

		log.Logf("Server received msg on stream: %s", msg.Id)

		wg.Add(1)
		go func(msg *pb.Message) {
			defer wg.Done()
			// Publish() launches another goroutine: we need to keep track of it
			if err := s.Mux.Publish(msg); err != nil {
				log.Logf("Error publishing on stream %s: %v", msg.Id, err)
			}
		}(msg)
	}

	// wait for all the publisher goroutines to finish
	wg.Wait()

	// remove the stream from Mux
	return s.Mux.RemoveStream(id)
}

func (s *Stream) Subscribe(ctx context.Context, req *pb.SubscribeRequest, stream pb.Stream_SubscribeStream) error {
	log.Logf("Received Stream.Subscribe request for stream: %s", req.Id)

	id := req.Id
	errCount := 0

	sub, err := sub.NewSubscriber(stream)
	if err != nil {
		return fmt.Errorf("Failed to create new subscriber for stream %s: %s", id, err)
	}

	if err := s.Mux.AddSub(id, sub); err != nil {
		return fmt.Errorf("Failed to add %v to stream: %s", sub.ID(), id)
	}

	for {
		select {
		case <-s.done:
			log.Logf("Stopping subscriber stream: %s", id)
			// clean up is done in Stop() function
			return nil
		case err := <-sub.ErrChan():
			if err != nil {
				log.Logf("Error receiving message on stream %s: %s", id, err)
				errCount++
			}

			if errCount > 5 {
				log.Logf("Error threshold reached for subscriber %s on stream: %s", sub.ID(), id)
				return s.Mux.RemSub(id, sub)
			}
		case <-sub.Done():
			// close the stream and return
			log.Logf("Closing subscriber stream: %s", id)
			return sub.Stream().Close()
		}
	}

	return nil
}

func (s *Stream) Stop() error {
	// notify running goroutines to stop
	close(s.done)

	// shut down multiplexer
	if err := s.Mux.Stop(); err != nil {
		return fmt.Errorf("Failed to stop stream multiplexer: %s", err)
	}

	return nil
}
