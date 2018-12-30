package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/google/uuid"
	k "github.com/segmentio/kafka-go"
	"log"
	"os"
	"time"
)

func main() {

	writer := flag.Bool("writer", false, "Writer option")
	partition := flag.Int("partition", 0, "Partition number for reader and writer")
	reader := flag.Bool("reader", false, "Reader option")
	unpart := flag.Bool("unpart", false, "Unpartitioned reader")
	v2test := flag.Bool("v2test", false, "v2 test")
	versions := flag.Bool("versions", false, "versions")
	topic := flag.String("topic", "test_topic", "Topic")
	flag.Parse()

	count := b2i(*writer) + b2i(*reader) + b2i(*unpart) + b2i(*v2test) + b2i(*versions)
	if count != 1 {
		fmt.Printf("We need exactly one command option\n")
		os.Exit(1)
	}

	switch {
	case *writer:
		fmt.Printf("This is writer. Partition %v\n", *partition)
		Writer(*topic, *partition)
	case *reader:
		fmt.Printf("This is reader. Partition %v\n", *partition)
		Reader(*topic, *partition)
	case *unpart:
		fmt.Printf("This is unpart\n")
		UnpartitionedReader(*topic)
	case *v2test:
		fmt.Println("V2 test\n")
		V2Test(*topic)
	case *versions:
		fmt.Println("Versions\n")
		Versions(*topic)
	}
}

func Versions(topic string) {
	conn, err := k.DialLeader(context.Background(), "tcp", "kafka:9092", topic, 0)
	if err != nil {
		log.Printf("Failed to connect to kafka cluster: %v", err)
		os.Exit(1)
	}
	versions, err := conn.ApiVersions()
	if err != nil {
		log.Printf("Error: %v", err)
	}

	for _, v := range versions {
		log.Printf("Api Key: %v, min: %v, max: %v", v.ApiKey, v.MinVersion, v.MaxVersion)
	}
	b := conn.ReadBatch(1, 1000)
	m, err := b.ReadMessage()
	if err != nil {
		log.Printf("Err: %v", err)
		return
	}
	log.Printf("Message: %v", m)
}

func V2Test(topic string) {
	msgs := make([]k.Message, 3)
	for i := range msgs {
		value := fmt.Sprintf("Hello World %d!", i)
		//msgs[i] = k.Message{Key: []byte("Key"), Value: []byte(value), Headers: []k.Header{k.Header{Key: "hk", Value: []byte("hv")}}}
		msgs[i] = k.Message{Key: []byte("Key"), Value: []byte(value)}
	}

	w := k.NewWriter(k.WriterConfig{
		Brokers:   []string{"kafka:9092"},
		Topic:     topic,
		BatchSize: 1,
	})
	defer w.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.WriteMessages(ctx, msgs...); err != nil {
		log.Fatalf("failed to produce messages: %+v", err)
		return
	}

	log.Printf("Success")

	r := k.NewReader(k.ReaderConfig{
		Brokers:   []string{"kafka:9092"},
		Topic:     topic,
		Partition: 0,
		MaxWait:   10 * time.Millisecond,
		MinBytes:  1,
		MaxBytes:  1000,
	})
	defer r.Close()

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Message: %v", string(m.Value))
	}
}

func b2i(x bool) int {
	if x {
		return 1
	} else {
		return 0
	}
}

func Reader(topic string, partition int) {
	conn, err := k.DialLeader(context.Background(), "tcp", "kafka:9092", topic, partition)
	if err != nil {
		log.Printf("Failed to connect to kafka cluster: %v", err)
		os.Exit(1)
	}

	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	batch := conn.ReadBatch(10e3, 1e6) // fetch 10KB min, 1MB max

	/*
		b := make([]byte, 10e3) // 10KB max per message
		for {
			_, err := batch.Read(b)
			if err != nil {
				break
			}
			fmt.Println(string(b))
		}
	*/
	for {
		m, err := batch.ReadMessage()
		if err != nil {
			log.Printf("Exited with message: %v", err)
			break
		}
		fmt.Println(m)
	}

	batch.Close()
	conn.Close()
}

func Writer(topic string, partition int) {
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
	for i := 0; i < 1000000; i++ {
		u, err := uuid.NewRandom()
		if err != nil {
			log.Printf("Failed to generate new uuid: %v", err)
		} else {
			conn.WriteMessages(
				k.Message{Key: []byte(u.String()), Value: []byte("onealsdfjkajsdflaksjdflaksdfjlkasdjflasdkjflaksdfjsd!")},
			)
		}

	}

	conn.Close()
}
