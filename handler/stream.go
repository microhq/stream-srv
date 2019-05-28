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

// NewStream creates new stream and  returns it.
// It returns error if stream multiplexer fails to be created.
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
	// keep track of publisher errors
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

		// initialize stream id
		id = msg.Id

		if errCount > 5 {
			log.Logf("Error threshold reached for stream: %s", id)
			break
		}

		log.Logf("Server received msg on stream: %s", id)

		wg.Add(1)
		go func(msg *pb.Message) {
			defer wg.Done()
			if err := s.Mux.Send(msg); err != nil {
				log.Logf("Error sending messag on stream %s: %v", id, err)
			}
		}(msg)
	}

	// remove the stream from Mux
	return s.Mux.DelStream(id, wg)
}

func (s *Stream) Subscribe(ctx context.Context, req *pb.SubscribeRequest, stream pb.Stream_SubscribeStream) error {
	log.Logf("Received Stream.Subscribe request for stream: %s", req.Id)

	id := req.Id

	sub, err := sub.NewSubscriber(stream)
	if err != nil {
		return fmt.Errorf("Failed to create new subscriber for stream %s: %s", id, err)
	}

	go func() {
		s.Mux.AddStreamSub(id, sub)
	}()

	for {
		select {
		case <-sub.DoneChan():
			// close the stream and return
			log.Logf("Closing subscriber %s stream: %s", sub.ID(), id)
			return sub.Stream().Close()
		case <-s.done:
			log.Logf("Stopping stream handler: %s", id)
			return nil
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
