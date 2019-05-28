package mux

import (
	"fmt"
	"sync"

	"github.com/micro/go-log"
	pb "github.com/microhq/stream-srv/proto/stream"
	"github.com/microhq/stream-srv/sub"
)

// Mux allows to multiplex streams to their subscribers
type Mux struct {
	// m maps stream to Sink
	m map[string]sub.Dispatcher
	// done notifies mux goroutines to stop
	done chan struct{}
	// wg keep strack of Mux goroutines
	wg *sync.WaitGroup
	sync.RWMutex
}

// New creates new Mux and returns it.
func New() (*Mux, error) {
	m := make(map[string]sub.Dispatcher)
	done := make(chan struct{})

	return &Mux{
		m:    m,
		done: done,
		wg:   new(sync.WaitGroup),
	}, nil
}

// AddStream adds new stream to Mux with given id and size of its buffer
func (m *Mux) AddStream(id string, size int) error {
	m.Lock()
	defer m.Unlock()

	if _, ok := m.m[id]; ok {
		return fmt.Errorf("Stream already exists: %s", id)
	}

	log.Logf("Adding new stream: %s", id)

	d, err := sub.NewDispatcher(id, size)
	if err != nil {
		return fmt.Errorf("Failed to create dispatcher for stream %s: %s", id, err)
	}

	// need to track all dispatcher goroutines
	m.wg.Add(1)

	// start dispatcher
	go d.Run(m.wg)

	m.m[id] = d

	return nil
}

// DelStream removes stream from Mux
func (m *Mux) DelStream(id string, wg *sync.WaitGroup) error {
	m.Lock()
	defer m.Unlock()

	if _, ok := m.m[id]; !ok {
		return fmt.Errorf("Stream does not exist: %s", id)
	}

	log.Logf("Removing stream: %s", id)

	if err := m.m[id].Stop(); err != nil {
		return fmt.Errorf("Failed to stop stream %s dispatcher: %s", id, err)
	}

	log.Logf("Waiting for stream publisher goroutine to finish...")

	// wait for all the publisher goroutines to finish
	wg.Wait()

	delete(m.m, id)

	return nil
}

// AddStreamSub adds new subscriber to stream
func (m *Mux) AddStreamSub(id string, s sub.Subscriber) error {
	m.RLock()
	defer m.RUnlock()

	if _, ok := m.m[id]; !ok {
		return fmt.Errorf("Stream does not exist: %s", id)
	}

	select {
	case m.m[id].CmdChan() <- &sub.Cmd{sub.Add, s}:
		log.Logf("Adding subscriber %s on stream: %s", s.ID(), id)
	case <-m.m[id].DoneChan():
		// do nothing here
	}

	return nil
}

// DelStreamSub deletes subscriber s from stream id
func (m *Mux) DelStreamSub(id string, s sub.Subscriber) error {
	m.RLock()
	defer m.RUnlock()

	if _, ok := m.m[id]; !ok {
		return fmt.Errorf("Stream does not exist: %s", id)
	}

	select {
	case m.m[id].CmdChan() <- &sub.Cmd{sub.Del, s}:
		log.Logf("Deleting subscriber %s from stream: %s", s.ID(), id)
	case <-m.m[id].DoneChan():
		// do nothing here
	}

	return nil
}

// Send sends the message downstream to dispatcher
func (m *Mux) Send(msg *pb.Message) error {
	m.RLock()
	defer m.RUnlock()

	if _, ok := m.m[msg.Id]; !ok {
		return fmt.Errorf("Stream does not exist: %s", m.m[msg.Id])
	}

	select {
	case m.m[msg.Id].MsgChan() <- msg:
		log.Logf("Sending message on stream: %s", msg.Id)
	case <-m.m[msg.Id].DoneChan():
		// do nothing here
	}

	return nil
}

// Stop stops all multiplexer dispatchers and waits for all goroutines to finish
func (m *Mux) Stop() error {
	m.Lock()
	defer m.Unlock()

	// stop all active dispatchers
	for id, _ := range m.m {
		log.Logf("Stopping stream dispatcher: %s", id)

		if err := m.m[id].Stop(); err != nil {
			return fmt.Errorf("Failed to stop stream %s dispatcher: %s", id, err)
		}

		delete(m.m, id)
	}

	// wait for all the goroutines to finish
	m.wg.Wait()

	return nil
}
