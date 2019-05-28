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
	// Stream returns data stream
	Stream() pb.Stream_SubscribeStream
	// Stop stops subscriber
	Stop() error
	// DoneChan returns stop notification channel
	DoneChan() <-chan struct{}
	// ErrCount increments subscriber errors
	ErrCount() int
}

// subscriber is a stream subscriber
type subscriber struct {
	// id is subscriber ID
	id uuid.UUID
	// stream is subscriber stream
	stream pb.Stream_SubscribeStream
	// done notifies subscriber to stop
	done chan struct{}
	// errCount counts errors
	errCount int
}

// NewSubscriber creates new stream subscriber and returns it
func NewSubscriber(stream pb.Stream_SubscribeStream) (*subscriber, error) {
	id := uuid.New()
	done := make(chan struct{})
	errCount := 0

	return &subscriber{
		id:       id,
		stream:   stream,
		done:     done,
		errCount: errCount,
	}, nil
}

// ID returns subscriber ID
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

	// close done channel to notify main goroutine
	close(s.done)

	return nil
}

// Done returns done channel
func (s *subscriber) DoneChan() <-chan struct{} {
	return s.done
}

// ErrChan returns subscriber error channel
func (s *subscriber) ErrCount() int {
	s.errCount += 1
	return s.errCount
}

// CmdAction defines command action
type CmdAction int

const (
	// AddSub adds new stream subscriber
	Add CmdAction = iota
	// DelSub deletes existing stream subscriber
	Del
)

// String implements Stringer interface
func (c CmdAction) String() string {
	switch c {
	case Add:
		return "Add"
	case Del:
		return "Del"
	default:
		return "Unknown"
	}
}

// Cmd is dispatcher command
type Cmd struct {
	// Cmd defines command
	Action CmdAction
	// Sub is subscriber
	Sub Subscriber
}

// Dispatcher dispatches stream data to stream subscribers
type Dispatcher interface {
	// Start starts message dispatcher
	Run(*sync.WaitGroup)
	// Stop stops dispatcher
	Stop() error
	// MsgChan returns message channel
	MsgChan() chan *pb.Message
	// CmdChan returns command channel
	CmdChan() chan *Cmd
	// DoneChan returns done channel
	DoneChan() chan struct{}
}

// dispatcher implements stream dispatcher
type dispatcher struct {
	// id is stream id
	id string
	// msg receives messages from upstream
	msg chan *pb.Message
	// cmd receives commands from upstream
	cmd chan *Cmd
	// done is a stop notification channel
	done chan struct{}
	// subs is a map of stream subscribers
	subs map[uuid.UUID]Subscriber
}

// NewDispatcher creates new message dispatcher
func NewDispatcher(id string, size int) (Dispatcher, error) {
	// bufferred message channel
	msg := make(chan *pb.Message, size)

	// create command channel
	cmd := make(chan *Cmd)

	// done notification channel
	done := make(chan struct{})

	// sMap is a map of stream subscribers
	subs := make(map[uuid.UUID]Subscriber)

	return &dispatcher{
		id:   id,
		msg:  msg,
		cmd:  cmd,
		done: done,
		subs: subs,
	}, nil
}

// Run runs message dispatcher
func (d *dispatcher) Run(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-d.done:
			log.Logf("Stopping dispatcher on stream: %s", d.id)
			return
		case cmd := <-d.cmd:
			log.Logf("Received %s command on stream: %s", cmd.Action, d.id)
			if err := d.runCmd(cmd); err != nil {
				log.Logf("Error running command %s on stream: %s", cmd.Action, d.id)
			}
		case msg := <-d.msg:
			log.Logf("Dispatching message to subscribers on stream: %s", d.id)
			for _, sub := range d.subs {
				if err := sub.Stream().Send(msg); err != nil {
					log.Logf("Error sending message to %s on stream: %s", sub.ID(), d.id)
					if sub.ErrCount() == 5 {
						log.Logf("Error threshold reached for subscriber %s on stream: %s", sub.ID(), d.id)
						if err := sub.Stop(); err != nil {
							log.Logf("Error stopping subscriber: %s", sub.ID())
						}
						delete(d.subs, sub.ID())
					}
				}
			}
		}
	}
}

// runCmd runs dispatcher command
func (d *dispatcher) runCmd(cmd *Cmd) error {
	switch cmd.Action {
	case Add:
		if _, ok := d.subs[cmd.Sub.ID()]; ok {
			return fmt.Errorf("Duplicate subscriber: %s", cmd.Sub.ID())
		}
		d.subs[cmd.Sub.ID()] = cmd.Sub
	case Del:
		if _, ok := d.subs[cmd.Sub.ID()]; !ok {
			return fmt.Errorf("Unknown subscriber: %s", cmd.Sub.ID())
		}
		delete(d.subs, cmd.Sub.ID())
	default:
		return fmt.Errorf("Unsupported command: %s", cmd.Action)
	}

	return nil
}

// MsgChan returns message channel
func (d *dispatcher) MsgChan() chan *pb.Message { return d.msg }

// CmdChan returns command channel
func (d *dispatcher) CmdChan() chan *Cmd { return d.cmd }

// DoneChan returns done channel
func (d *dispatcher) DoneChan() chan struct{} { return d.done }

// Stop stops dispatcher
func (d *dispatcher) Stop() error {
	log.Logf("Attempting to stop dispatcher on stream: %s", d.id)

	// close the channels
	close(d.done)

	// notify all subscribers to finish
	for _, s := range d.subs {
		if err := s.Stop(); err != nil {
			return fmt.Errorf("Failed to stop subscriber: %s", s.ID())
		}
	}

	for range d.msg {
		// do nothing here; just drain messge channel
	}

	return nil
}
