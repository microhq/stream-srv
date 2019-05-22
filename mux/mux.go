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
	m map[string]*sub.Subscription
	sync.Mutex
}

// New creates new Mux and returns it
func New() (*Mux, error) {
	m := make(map[string]*sub.Subscription)

	return &Mux{m: m}, nil
}

// Add adds new stream to Mux
func (m *Mux) Add(id string) error {
	m.Lock()
	defer m.Unlock()

	if _, ok := m.m[id]; ok {
		return fmt.Errorf("Stream already exists: %s", id)
	}

	log.Logf("Adding stream: %s", id)

	s, err := sub.New(id)
	if err != nil {
		return fmt.Errorf("Failed to add stream %s: %s", id, err)
	}

	m.m[id] = s

	return nil
}

// Remove removes stream from Mux
func (m *Mux) Remove(id string) error {
	m.Lock()
	defer m.Unlock()

	if _, ok := m.m[id]; ok {
		return fmt.Errorf("Stream does not exist: %s", id)
	}

	log.Logf("Removing stream: %s", id)
	delete(m.m, id)

	return nil
}

// AddSub adds new subscriber to stream id
func (m *Mux) AddSub(id string, s *sub.Subscriber) error {
	m.Lock()
	defer m.Unlock()

	if _, ok := m.m[id]; !ok {
		return fmt.Errorf("Stream does not exist: %s", id)
	}

	if err := m.m[id].GetSubs().Add(s); err != nil {
		return err
	}

	return nil
}

// RemSub removes subscriber from stream id
func (m *Mux) RemSub(id string, s *sub.Subscriber) error {
	m.Lock()
	defer m.Unlock()

	if _, ok := m.m[id]; !ok {
		return fmt.Errorf("Stream does not exist: %s", id)
	}

	if err := m.m[id].GetSubs().Remove(s.ID); err != nil {
		return err
	}

	return nil
}

// GetSubscription returns subscription for given stream id
func (m *Mux) GetSubscription(id string) (*sub.Subscription, error) {
	m.Lock()
	defer m.Unlock()

	if _, ok := m.m[id]; !ok {
		return nil, fmt.Errorf("Stream does not exist: %s", id)
	}

	return m.m[id], nil
}

// GetChan returns message channel for given stream
func (m *Mux) GetChan(id string) (chan *pb.Message, error) {
	if _, ok := m.m[id]; !ok {
		return nil, fmt.Errorf("Stream does not exist: %s", id)
	}

	return m.m[id].GetChan(), nil
}
