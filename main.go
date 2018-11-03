package main

import (
	"context"
	"flag"
	"fmt"
	k "github.com/segmentio/kafka-go"
	"os"
	"time"
)

func main() {

	writer := flag.Bool("writer", false, "Writer option")
	reader := flag.Bool("reader", false, "Reader option")
	unpart := flag.Bool("unpart", false, "Unpartitioned reader")
	flag.Parse()

	count := b2i(*writer) + b2i(*reader) + b2i(*unpart)
	if count != 1 {
		fmt.Printf("We need exactly one command option\n")
		os.Exit(1)
	}

	topic := "new_topic"

	switch {
	case *writer:
		fmt.Printf("This is writer\n")
		Writer(topic)
	case *reader:
		fmt.Printf("This is reader\n")
		Reader(topic)
	case *unpart:
		fmt.Printf("This is unpart\n")
		UnpartitionedReader(topic)
	}
}

func b2i(x bool) int {
	if x {
		return 1
	} else {
		return 0
	}
}

func Reader(topic string) {
	partition := 0

	conn, _ := k.DialLeader(context.Background(), "tcp", "kafka:9092", topic, partition)

	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	batch := conn.ReadBatch(10e3, 1e6) // fetch 10KB min, 1MB max

	b := make([]byte, 10e3) // 10KB max per message
	for {
		_, err := batch.Read(b)
		if err != nil {
			break
		}
		fmt.Println(string(b))
	}

	batch.Close()
	conn.Close()
}

func Writer(topic string) {
	partition := 0

	conn, err := k.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	if err != nil {
		fmt.Printf("Failed to connect writer %v", err)
		os.Exit(1)
	}

	err = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if err != nil {
		fmt.Printf("Failed to set deadline %v", err)
		os.Exit(1)
	}
	for i := 0; i < 1000; i++ {
		conn.WriteMessages(
			k.Message{Key: []byte("509e9b00-de75-11e8-9f32-f2801f1b9fd1"), Value: []byte("one!")},
			k.Message{Key: []byte("509ea41a-de75-11e8-9f32-f2801f1b9fd1"), Value: []byte("two!")},
			k.Message{Key: []byte("509ea5fa-de75-11e8-9f32-f2801f1b9fd1"), Value: []byte("three!")},
		)

	}

	conn.Close()
}
