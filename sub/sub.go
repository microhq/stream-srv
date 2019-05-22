package sub

import (
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/micro/go-log"
	pb "github.com/microhq/stream-srv/proto/stream"
)

// Subscriber is a stream subscriber
type Subscriber struct {
	// ID is subscriber ID
	ID uuid.UUID
	// stream is Subscriber's stream
	stream pb.Stream_SubscribeStream
	// errChan is error channel
	errChan chan error
}

func NewSubscriber(stream pb.Stream_SubscribeStream) (*Subscriber, error) {
	ID := uuid.New()
	errChan := make(chan error, 1)

	return &Subscriber{
		ID:      ID,
		stream:  stream,
		errChan: errChan,
	}, nil
}

// GetID returns subscriber's ID
func (s *Subscriber) GetID() uuid.UUID {
	return s.ID
}

// GetStream returns subscriber's stream
func (s *Subscriber) GetStream() pb.Stream_SubscribeStream {
	return s.stream
}

// GetErrChan returns subscriber's error channel
func (s *Subscriber) GetErrChan() chan error {
	return s.errChan
}

// Subscribers is a stream subscription
type Subscribers struct {
	// smap is a map of subscribers
	smap map[uuid.UUID]*Subscriber
	sync.Mutex
}

// Add adds a new subscriber
func (s *Subscribers) Add(_s *Subscriber) error {
	s.Lock()
	defer s.Unlock()

	log.Logf("Adding subscriber: %s", _s.ID)

	if _, ok := s.smap[_s.ID]; ok {
		return fmt.Errorf("Subscriber already exists: %v", _s.ID)
	}
	s.smap[_s.ID] = _s

	return nil
}

// Remove removes subscriber
func (s *Subscribers) Remove(id uuid.UUID) error {
	s.Lock()
	defer s.Unlock()

	log.Logf("Removing subscriber %s", id)

	delete(s.smap, id)

	return nil
}

// Get returns a subscriber with id
func (s *Subscribers) Get(id uuid.UUID) *Subscriber {
	s.Lock()
	defer s.Unlock()

	log.Log("Retrieveing subscriber: %s", id)

	return s.smap[id]
}

// AsList returns a slice of all subscribers
func (s *Subscribers) AsList() []*Subscriber {
	s.Lock()
	defer s.Unlock()

	log.Log("Retrieveing subscribers")

	subs := make([]*Subscriber, len(s.smap))

	i := 0
	for _, sub := range s.smap {
		subs[i] = sub
		i++
	}

	log.Logf("Subscribers retrieved: %d", len(subs))

	return subs
}

// Subscription provides a stream subscription
type Subscription struct {
	// id is stream id
	id string
	// ch is channel for streaming messages
	ch chan *pb.Message
	// subs is a map of subscribers
	subs *Subscribers
}

// New creates new subscription
func New(id string) (*Subscription, error) {
	ch := make(chan *pb.Message)
	smap := make(map[uuid.UUID]*Subscriber)
	subs := &Subscribers{smap: smap}

	return &Subscription{
		id:   id,
		ch:   ch,
		subs: subs,
	}, nil
}

// GetChan returns subscription channel
func (s *Subscription) GetChan() chan *pb.Message {
	return s.ch
}

// GetSubs returns all subscription subscribers
func (s *Subscription) GetSubs() *Subscribers {
	return s.subs
}

// GetID gets Subscription ID
func (s *Subscription) GetID() string {
	return s.id
}
