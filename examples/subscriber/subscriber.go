package main

import (
	"context"

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

	for {
		msg, err := stream.Recv()
		if err != nil {
			log.Logf("Error receiving message from stream: %d", id)
			continue
		}
		log.Logf("Received message from stream %d: %v", id, msg)
	}
}
