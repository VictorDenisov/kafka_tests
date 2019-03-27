package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/google/uuid"
	k "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/gzip"
	"github.com/segmentio/kafka-go/lz4"
	"github.com/segmentio/kafka-go/snappy"
	"log"
	"os"
	"time"
)

func main() {

	writer := flag.Bool("writer", false, "Writer option")
	partition := flag.Int("partition", 0, "Partition number for reader and writer")
	reader := flag.Bool("reader", false, "Reader option")
	connReader := flag.Bool("connReader", false, "Conn Reader option")
	bugProducer := flag.Bool("bugProducer", false, "Bug Producer option")
	bugConsumer := flag.Bool("bugConsumer", false, "Bug Consumer option")
	unpart := flag.Bool("unpart", false, "Unpartitioned reader")
	v2test := flag.Bool("v2test", false, "v2 test")
	versions := flag.Bool("versions", false, "versions")
	topic := flag.String("topic", "test_topic", "Topic")
	flag.Parse()

	count := b2i(*writer) + b2i(*reader) + b2i(*unpart) + b2i(*v2test) + b2i(*versions) + b2i(*connReader) + b2i(*bugConsumer) + b2i(*bugProducer)
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
	case *connReader:
		fmt.Printf("This is connReader. Partition %v\n", *partition)
		ConnReader(*topic, *partition)
	case *bugProducer:
		fmt.Printf("This is bugProducer. Partition %v\n", *partition)
		BugProducer(*topic, *partition)
	case *bugConsumer:
		fmt.Printf("This is bugConsumer. Partition %v\n", *partition)
		BugConsumer(*topic, *partition)
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
	offset := 0

	produce := func(n int, codec k.CompressionCodec) {
		w := k.NewWriter(k.WriterConfig{
			Brokers:          []string{"kafka:9092"},
			Topic:            topic,
			CompressionCodec: codec,
		})
		defer w.Close()

		msgs := make([]k.Message, n)
		for i := range msgs {
			value := fmt.Sprintf("Bye World %d!", offset)
			offset++
			msgs[i] = k.Message{Value: []byte(value)}
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := w.WriteMessages(ctx, msgs...); err != nil {
			log.Printf("failed to produce messages: %+v", err)
		}
	}

	produce(3, nil)
	produce(4, gzip.NewCompressionCodec())
	produce(2, nil)
	produce(4, lz4.NewCompressionCodec())
	produce(10, snappy.NewCompressionCodec())

	log.Printf("Success")

	/*
			r := k.NewReader(k.ReaderConfig{

		log.Printf("Success")

		/*
			r := k.NewReader(k.ReaderConfig{
				Brokers:   []string{"kafka:9092"},
				Topic:     topic,
				Partition: 0,
				MaxWait:   10 * time.Millisecond,
				MinBytes:  1,
				MaxBytes:  1000,
			})
			defer r.Close()

			for {
				m, err := r.ReadMessage(context.Background())
				if err != nil {
					log.Fatal(err)
				}
				log.Printf("Message: %v", string(m.Value))
				log.Printf("================")
			}
	*/
}

func b2i(x bool) int {
	if x {
		return 1
	} else {
		return 0
	}
}

func Reader(topic string, partition int) {
	r := k.NewReader(k.ReaderConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     topic,
		Partition: 0,
		//MaxWait:  10 * time.Millisecond,
		MinBytes:       1,
		MaxBytes:       1000,
		IsolationLevel: k.ReadCommitted,
	})
	defer r.Close()

	//r.SetOffset(4)
	first := true
	offset := int64(0)
	for {
		r.SetOffset(offset)
		offset++
		//log.Printf("Starting message reading loop")
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		if first {
			//r.SetOffset(2)
			first = false
		}
		log.Printf("Message: %v", string(m.Value))
		log.Printf("Offset: %v", m.Offset)
		for _, h := range m.Headers {
			log.Printf("%v - %v", h.Key, string(h.Value))
		}
		log.Printf("================")
	}
	/*
		conn, err := k.DialLeader(context.Background(), "tcp", "kafka:9092", topic, partition)
		if err != nil {
			log.Printf("Failed to connect to kafka cluster: %v", err)
			os.Exit(1)
		}

		for {
			conn.SetReadDeadline(time.Now().Add(10 * time.Second))
			conn.Seek(3, k.SeekAbsolute)
			batch := conn.ReadBatch(10e3, 1e6) // fetch 10KB min, 1MB max
			for {
				m, err := batch.ReadMessage()
				if err != nil {
					log.Printf("Exited with message: %v", err)
					break
				}
				fmt.Printf("Message: %v\n", string(m.Value))
				fmt.Printf("---- Headers: %v\n", m.Headers)
			}
			batch.Close()
		}

		conn.Close()
	*/
}

func ConnReader(topic string, partition int) {
	conn, err := k.DialLeader(context.Background(), "tcp", "kafka:9092", topic, partition)
	if err != nil {
		log.Printf("Failed to connect to kafka cluster: %v", err)
		os.Exit(1)
	}
	b := make([]byte, 100)

	start := time.Now()
	deadline := start.Add(250 * time.Millisecond)

	conn.SetReadDeadline(deadline)
	_, err = conn.Read(b)
	log.Printf("Error: %v", err)
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

func BugConsumer(topic string, partition int) {
	conn, _ := k.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)

	conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	batch := conn.ReadBatch(1, 1e6) // fetch 10KB min, 1MB max

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

var a = `Contrary to popular belief, Lorem Ipsum is not simply random text. It has roots in a piece of classical Latin literature from 45 BC, making it over 2000 years old. Richard McClintock, a Latin professor at Hampden-Sydney College in Virginia, looked up one of the more obscure Latin words, consectetur, from a Lorem Ipsum passage, and going through the cites of the word in classical literature, discovered the undoubtable source. Lorem Ipsum comes from sections 1.10.32 and 1.10.33 of "de Finibus Bonorum et Malorum" (The Extremes of Good and Evil) by Cicero, written in 45 BC. This book is a treatise on the theory of ethics, very popular during the Renaissance. The first line of Lorem Ipsum, "Lorem ipsum dolor sit amet..", comes from a line in section 1.10.32. The standard chunk of Lorem Ipsum used since the 1500s is reproduced below for those interested. Sections 1.10.32 and 1.10.33 from "de Finibus Bonorum et Malorum" by Cicero are also reproduced in their exact original form, accompanied by English versions from the 1914 translation by H. Rackham.`

func BugProducer(topic string, partition int) {
	conn, _ := k.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)

	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	conn.WriteMessages(
		k.Message{Value: []byte(a)},
	)

	conn.Close()
}
