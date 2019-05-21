package handler

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
	// errCount is error counter
	errCount int
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

// Get returns a slice of all subscribers
func (s *Subscribers) GetAll() []*Subscriber {
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
	id int64
	// ch is channel for streaming messages
	ch chan *pb.Msg
	// subs is a map of subscribers
	subs *Subscribers
}

// GetChan returns subscription channel
func (s *Subscription) GetChan() chan *pb.Msg {
	return s.ch
}

// GetSubs returns all subscription subscribers
func (s *Subscription) GetSubs() []*Subscriber {
	return s.subs.GetAll()
}

// GetID gets Subscription ID
func (s *Subscription) GetID() int64 {
	return s.id
}

// Mux allows to multiplex streams to their subscribers
type Mux struct {
	// m maps stream to subscriptions
	m map[int64]*Subscription
	sync.Mutex
}

// New creates new Mux and returns it
func NewMux() (*Mux, error) {
	m := make(map[int64]*Subscription)

	return &Mux{m: m}, nil
}

// Add adds new stream to Mux
func (m *Mux) Add(id int64) error {
	m.Lock()
	defer m.Unlock()

	if _, ok := m.m[id]; ok {
		return fmt.Errorf("Stream already exists: %d", id)
	}

	log.Logf("Adding stream: %d", id)

	ch := make(chan *pb.Msg)
	smap := make(map[uuid.UUID]*Subscriber)
	subs := &Subscribers{smap: smap}
	m.m[id] = &Subscription{id: id, ch: ch, subs: subs}

	return nil
}

// Remove removes stream from Mux
func (m *Mux) Remove(id int64) error {
	m.Lock()
	defer m.Unlock()

	if _, ok := m.m[id]; ok {
		return fmt.Errorf("Stream does not exist: %d", id)
	}

	log.Logf("Removing stream: %d", id)
	delete(m.m, id)

	return nil
}

// AddSub adds new subscriber to stream id
func (m *Mux) AddSub(id int64, s *Subscriber) error {
	m.Lock()
	defer m.Unlock()

	if _, ok := m.m[id]; !ok {
		return fmt.Errorf("Stream does not exist: %d", id)
	}

	if err := m.m[id].subs.Add(s); err != nil {
		return err
	}

	return nil
}

// RemSub removes subscriber from stream id
func (m *Mux) RemSub(id int64, s *Subscriber) error {
	m.Lock()
	defer m.Unlock()

	if _, ok := m.m[id]; !ok {
		return fmt.Errorf("Stream does not exist: %d", id)
	}

	if err := m.m[id].subs.Remove(s.ID); err != nil {
		return err
	}

	return nil
}

// GetSubscription returns subscription for given stream id
func (m *Mux) GetSubscription(id int64) (*Subscription, error) {
	m.Lock()
	defer m.Unlock()

	if _, ok := m.m[id]; !ok {
		return nil, fmt.Errorf("Stream does not exist: %d", id)
	}

	return m.m[id], nil
}

// GetChan returns message channel for given stream
func (m *Mux) GetChan(id int64) (chan *pb.Msg, error) {
	if _, ok := m.m[id]; !ok {
		return nil, fmt.Errorf("Stream does not exist: %d", id)
	}

	return m.m[id].GetChan(), nil
}
