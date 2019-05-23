package main

import (
	"context"
	"io"

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
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			log.Logf("Stream publisher disconnected")
			break
		}

		if err != nil {
			log.Logf("Error receiving message from stream: %s", id)
			retries--
		}

		if retries == 0 {
			log.Logf("Reached retry threshold, bailing...")
			break
		}

		log.Logf("Received message from stream %s: %v", id, msg)
	}
}
