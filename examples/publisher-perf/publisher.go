package main

import (
	"context"
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
	_, err := client.Create(context.Background(), &pb.CreateRequest{Id: id})
	if err != nil {
		log.Fatal(err)
	}

	stream, err := client.Publish(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	ticker := time.NewTicker(1 * time.Second)
	start := time.Now()

	prevCount := 0
	i := 0
	for {
		select {
		case <-ticker.C:
			elapsed := time.Since(start)
			log.Logf("Streaming speed: %.2f/s", float64(i-prevCount)/(elapsed.Seconds()))
			prevCount = i
			start = time.Now()
		default:
			if err := stream.Send(&pb.Message{Id: id}); err != nil {
				log.Logf("Error sending %dth message: %s", i, err)
			}
			i++
		}
		//time.Sleep(500 * time.Millisecond)
	}
}
