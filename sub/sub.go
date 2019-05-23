package sub

import (
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/micro/go-log"
	pb "github.com/microhq/stream-srv/proto/stream"
)

type Subscriber interface {
	// ID returns subscriber ID
	ID() uuid.UUID
	// Stream returns stream
	Stream() pb.Stream_SubscribeStream
	// Stop stops subscriber
	Stop() error
	// Done returns done channel
	Done() <-chan struct{}
	// ErrChan returns error channel
	ErrChan() chan error
}

// subscriber is a stream subscriber
type subscriber struct {
	// id is subscriber ID
	id uuid.UUID
	// stream is subscriber stream
	stream pb.Stream_SubscribeStream
	// done notifies subscriber to stop
	done chan struct{}
	// errChan is error channel
	errChan chan error
}

func NewSubscriber(stream pb.Stream_SubscribeStream) (*subscriber, error) {
	id := uuid.New()
	done := make(chan struct{})
	errChan := make(chan error, 1)

	return &subscriber{
		id:      id,
		stream:  stream,
		done:    done,
		errChan: errChan,
	}, nil
}

// ID returns subscriber's ID
func (s *subscriber) ID() uuid.UUID {
	return s.id
}

// Stream returns subscriber stream
func (s *subscriber) Stream() pb.Stream_SubscribeStream {
	return s.stream
}

// Stop closes subscriber channel
func (s *subscriber) Stop() error {
	log.Logf("Stopping subscriber: %s", s.id)
	close(s.done)
	return nil
}

// Done returns done channel
func (s *subscriber) Done() <-chan struct{} {
	return s.done
}

// ErrChan returns subscriber error channel
func (s *subscriber) ErrChan() chan error {
	return s.errChan
}

// Subscribers manages subscribers
type Subscribers interface {
	// Add adds new subscriber
	Add(Subscriber) error
	// Remove removes subscriber
	Remove(uuid.UUID) error
	// Get returns subscriber
	Get(uuid.UUID) Subscriber
	// AsList returns list of subscribers
	AsList() []Subscriber
}

// subscribers is a map of stream subscribers
type subscribers struct {
	// sMap is a map of subscribers
	sMap map[uuid.UUID]Subscriber
	sync.Mutex
}

// Add adds a new subscriber
func (s *subscribers) Add(_s Subscriber) error {
	s.Lock()
	defer s.Unlock()

	if _, ok := s.sMap[_s.ID()]; ok {
		return fmt.Errorf("Subscriber already exists: %v", _s.ID())
	}

	s.sMap[_s.ID()] = _s

	return nil
}

// Remove removes subscriber
func (s *subscribers) Remove(id uuid.UUID) error {
	s.Lock()
	defer s.Unlock()

	delete(s.sMap, id)

	return nil
}

// Get returns subscriber with id
func (s *subscribers) Get(id uuid.UUID) Subscriber {
	s.Lock()
	defer s.Unlock()

	log.Log("Retrieveing subscriber: %s", id)

	return s.sMap[id]
}

// AsList returns a slice of all subscribers
func (s *subscribers) AsList() []Subscriber {
	s.Lock()
	defer s.Unlock()

	subs := make([]Subscriber, len(s.sMap))

	i := 0
	for _, sub := range s.sMap {
		subs[i] = sub
		i++
	}

	log.Logf("Subscribers detected: %d", len(subs))

	return subs
}

// Dispatcher dispatches stream data to stream subscribers
type Dispatcher interface {
	// Start starts message dispatcher
	Start(*sync.WaitGroup)
	// Subscribers returns subscribers
	Subscribers() Subscribers
	// Dispatch dispatches the message
	Dispatch(*pb.Message) error
	// Stop stops dispatcher
	Stop() error
}

// TODO: Dispatcher should have a worker pool
// dispatcher implements stream dispatcher
type dispatcher struct {
	// id is stream id
	id string
	// in receives messages from publisher
	in chan *pb.Message
	// done is a stop notification channel
	done chan struct{}
	// s is a map of stream subscribers
	s *subscribers
}

// NewDispatcher creates new message dispatcher
func NewDispatcher(id string, size int) (Dispatcher, error) {
	// bufferred message channel
	in := make(chan *pb.Message, size)
	// done notification channel
	done := make(chan struct{})
	// sMap is a map of stream subscribers
	sMap := make(map[uuid.UUID]Subscriber)
	s := &subscribers{sMap: sMap}

	return &dispatcher{
		id:   id,
		in:   in,
		done: done,
		s:    s,
	}, nil
}

// Start starts message dispatcher
func (d *dispatcher) Start(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-d.done:
			log.Logf("Stopping dispatcher for stream: %s", d.id)
			return
		case msg := <-d.in:
			log.Logf("Dispatching message to subscribers on stream: %s", d.id)
			for _, sub := range d.s.AsList() {
				if err := sub.Stream().Send(msg); err != nil {
					// send the error down subscriber error channel
					sub.ErrChan() <- err
				}
			}
		}
	}
}

// Dispatch dispatches the message to the channel
func (d *dispatcher) Dispatch(msg *pb.Message) error {
	select {
	case <-d.done:
		// we're done here; close the channel
		return nil
	case d.in <- msg:
	}
	return nil
}

// Subscribers returns a list of subscribers
func (d *dispatcher) Subscribers() Subscribers {
	return d.s
}

// Stop stops dispatcher
func (d *dispatcher) Stop() error {
	// close the channels
	close(d.done)
	close(d.in)

	// notify all subscribers to finish
	for _, s := range d.s.sMap {
		if err := s.Stop(); err != nil {
			return fmt.Errorf("Failed to stop subscriber: %s", s.ID())
		}
	}

	// drain incoming message channel
	for range d.in {
		// do nothing here
	}

	return nil
}
