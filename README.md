# Stream Service

This is the Stream service

Generated with

```
micro new stream-srv --namespace=go.micro --type=srv
```

## Getting Started

- [Configuration](#configuration)
- [Dependencies](#dependencies)
- [Usage](#usage)
- [Example](#example)

## Configuration

- FQDN: go.micro.srv.stream
- Type: srv
- Alias: stream

## Dependencies

Micro services depend on service discovery. The default is multicast DNS, a zeroconf system.

In the event you need a resilient multi-host setup we recommend consul.

```
# install consul
brew install consul

# run consul
consul agent -dev
```

## Usage

A Makefile is included for convenience

Build the binary

```
make build
```

Run the service
```
./stream-srv
```

Build a docker image
```
make docker
```

## Example

Run stream service:
```
$ go run src/github.com/microhq/stream-srv
2019/05/21 15:52:45 Transport [http] Listening on [::]:53944
2019/05/21 15:52:45 Broker [http] Connected to [::]:53945
2019/05/21 15:52:45 Registry [consul] Registering node: go.micro.srv.stream-b1d13674-e611-4fab-9ee4-dfe0e8ca6dc8
```

Run publisher:
```
$ MICRO_REGISTRY=consul go run publisher.go
2019/05/21 15:53:37 Publishing message 0 to stream: 1
2019/05/21 15:53:38 Publishing message 1 to stream: 1
2019/05/21 15:53:39 Publishing message 2 to stream: 1
2019/05/21 15:53:40 Publishing message 3 to stream: 1
2019/05/21 15:53:41 Publishing message 4 to stream: 1
2019/05/21 15:53:42 Publishing message 5 to stream: 1
```

Run subscriber(s):
```
$ MICRO_REGISTRY=consul go run subscriber.go
2019/05/21 15:54:26 Received message from stream 1: id:1
2019/05/21 15:54:27 Received message from stream 1: id:1
2019/05/21 15:54:28 Received message from stream 1: id:1
2019/05/21 15:54:29 Received message from stream 1: id:1
2019/05/21 15:54:30 Received message from stream 1: id:1
2019/05/21 15:54:31 Received message from stream 1: id:1
```
