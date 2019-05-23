package main

import (
	"context"
	"io"
	"time"

	"github.com/micro/go-log"
	"github.com/micro/go-micro"
	pb "github.com/microhq/stream-srv/proto/stream"
)

func main() {
	// New Service
	service := micro.NewService(
		micro.Name("go.micro.srv.stream"),
		micro.Version("latest"),
	)

	// Initialise service
	service.Init()

	client := pb.NewStreamService("go.micro.srv.stream", service.Client())

	id := "1"

	stream, err := client.Subscribe(context.Background(), &pb.SubscribeRequest{Id: id})
	if err != nil {
		log.Fatal(err)
	}

	retries := 5

	ticker := time.NewTicker(1 * time.Second)
	start := time.Now()

	prevCount := 0
	i := 0
stop:
	for {
		select {
		case <-ticker.C:
			elapsed := time.Since(start)
			log.Logf("Streaming speed: %.2f/s", float64(i-prevCount)/(elapsed.Seconds()))
			prevCount = i
			start = time.Now()
		default:
			_, err := stream.Recv()
			if err == io.EOF {
				log.Logf("Stream publisher disconnected")
				break stop
			}

			if err != nil {
				log.Logf("Error receiving message from stream: %s", id)
				retries--
			}

			if retries == 0 {
				log.Logf("Reached retry threshold, bailing...")
				break stop
			}
			//log.Logf("Received message from stream %s: %v", id, msg)
			i++
		}
		//time.Sleep(500 * time.Millisecond)
	}

}
