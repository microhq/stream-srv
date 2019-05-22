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
	// m maps stream to Sink
	m map[int64]sub.Dispatcher
	// wg keep strack of Mux goroutines
	wg *sync.WaitGroup
	sync.Mutex
}

// New creates new Mux and returns it
func New() (*Mux, error) {
	m := make(map[int64]sub.Dispatcher)

	return &Mux{
		m:  m,
		wg: new(sync.WaitGroup),
	}, nil
}

// AddStream adds new stream to Mux with given id and size of its buffer
func (m *Mux) AddStream(id int64, size int) error {
	m.Lock()
	defer m.Unlock()

	if _, ok := m.m[id]; ok {
		return fmt.Errorf("Stream already exists: %d", id)
	}

	log.Logf("Adding new stream: %d", id)

	d, err := sub.NewDispatcher(id, size)
	if err != nil {
		return fmt.Errorf("Failed to create dispatcher for stream %d: %s", id, err)
	}

	// need to track all dispatcher goroutines
	m.wg.Add(1)
	// start dispatcher
	go d.Start(m.wg)

	m.m[id] = d

	return nil
}

// RemoveStream removes stream from Mux
func (m *Mux) RemoveStream(id int64) error {
	m.Lock()
	defer m.Unlock()

	if _, ok := m.m[id]; ok {
		return fmt.Errorf("Stream does not exist: %d", id)
	}

	log.Logf("Removing stream: %d", id)

	if err := m.m[id].Stop(); err != nil {
		return fmt.Errorf("Failed to stop stream %d dispatched: %s", id, err)
	}

	delete(m.m, id)

	return nil
}

// AddSub adds new subscriber to stream id
func (m *Mux) AddSub(id int64, s sub.Subscriber) error {
	m.Lock()
	defer m.Unlock()

	if _, ok := m.m[id]; !ok {
		return fmt.Errorf("Stream does not exist: %d", id)
	}

	if err := m.m[id].Subscribers().Add(s); err != nil {
		return err
	}

	return nil
}

// RemSub removes subscriber from stream id
func (m *Mux) RemSub(id int64, s sub.Subscriber) error {
	m.Lock()
	defer m.Unlock()

	if _, ok := m.m[id]; !ok {
		return fmt.Errorf("Stream does not exist: %d", id)
	}

	if err := m.m[id].Subscribers().Remove(s.ID()); err != nil {
		return err
	}

	return nil
}

// Publish sends the message down to dispatcher
func (m *Mux) Publish(msg *pb.Msg) error {
	m.Lock()
	defer m.Unlock()

	id := msg.Id
	if _, ok := m.m[id]; !ok {
		return fmt.Errorf("Stream does not exist: %d", id)
	}

	return m.m[id].Dispatch(msg)
}

// Stop stops Mux
func (m *Mux) Stop() error {
	m.Lock()
	defer m.Unlock()

	// stop all active dispatchers
	for id, _ := range m.m {
		if err := m.RemoveStream(id); err != nil {
			return fmt.Errorf("Failed to remove stream %d: %s", id, err)
		}
	}

	m.wg.Wait()
	return nil
}
