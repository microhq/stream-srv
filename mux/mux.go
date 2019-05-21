package mux

import (
	"fmt"
	"sync"

	"github.com/go-log/log"
	pb "github.com/microhq/stream-srv/proto/stream"
	"github.com/microhq/stream-srv/sub"
)

// Mux allows to multiplex streams to their subscribers
type Mux struct {
	// m maps stream to subscriptions
	m map[int64]*sub.Subscription
	sync.Mutex
}

// New creates new Mux and returns it
func New() (*Mux, error) {
	m := make(map[int64]*sub.Subscription)

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

	s, err := sub.New(id)
	if err != nil {
		return fmt.Errorf("Failed to add stream %d: %s", id, err)
	}

	m.m[id] = s

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
func (m *Mux) AddSub(id int64, s *sub.Subscriber) error {
	m.Lock()
	defer m.Unlock()

	if _, ok := m.m[id]; !ok {
		return fmt.Errorf("Stream does not exist: %d", id)
	}

	if err := m.m[id].GetSubs().Add(s); err != nil {
		return err
	}

	return nil
}

// RemSub removes subscriber from stream id
func (m *Mux) RemSub(id int64, s *sub.Subscriber) error {
	m.Lock()
	defer m.Unlock()

	if _, ok := m.m[id]; !ok {
		return fmt.Errorf("Stream does not exist: %d", id)
	}

	if err := m.m[id].GetSubs().Remove(s.ID); err != nil {
		return err
	}

	return nil
}

// GetSubscription returns subscription for given stream id
func (m *Mux) GetSubscription(id int64) (*sub.Subscription, error) {
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
