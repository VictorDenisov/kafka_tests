package main

import (
	"context"
	k "github.com/segmentio/kafka-go"
	"io"
	"log"
	"reflect"
	"time"
)

func UnpartitionedReader(topic string) {

	brokers := []string{"kafka:9092"}

	var partitions []k.Partition = nil
	for _, b := range brokers {
		log.Printf("Connecting to broker: %v", b)
		if conn, err := k.DialContext(context.Background(), "tcp", b); err == nil {
			partitions, err = conn.ReadPartitions(topic)
			if err != nil {
				log.Printf("Failed to connect: %v", err)
				continue
			} else {
				break
			}

		} else {
			log.Printf("Failed to connect: %v", err)
		}
	}
	log.Printf("Received partitions are: %v", partitions)
	for _, p := range partitions {
		//conn, err := DialPartition(context.Background(), "tcp", k.DefaultDialer, p)
		log.Printf("Dialing partition %v", p)
		conn, err := k.DialPartition(context.Background(), "tcp", "kafka:9092", p)
		if err != nil {
			log.Printf("Failed to dial leader: %v", err)
		}
		const safetyTimeout = 10 * time.Second
		deadline := time.Now().Add(safetyTimeout)
		conn.SetReadDeadline(deadline)
		_, err = ReadMessages(conn)
		if err != nil {
			log.Printf("Failed to read messages: %v", err)
		}

		/*
			for _, m := range msgs {
				log.Printf("Key: %v - %v", string(m.Key), string(m.Value))
			}
		*/
	}
}

func ReadMessages(conn *k.Conn) ([]k.Message, error) {
	var msgs []k.Message
	for {
		batch := conn.ReadBatch(0, 100)

		msgCount := 0
	loop:
		for {
			msg, err := batch.ReadMessage()
			if err != nil {
				switch err := err.(type) {
				case k.Error:
					log.Printf("This is kafka error")
					if err.Timeout() {
						return msgs, err
					} else {
						return msgs, err
					}
				default:
					if err == io.EOF {
						err := batch.Close()
						if err != nil {
							log.Printf("Failed to close batch")
							return msgs, err
						}
						if msgCount == 0 {
							return msgs, nil
						} else {
							//log.Printf("msgCount is not 0. Exiting loop.")
							break loop
						}
					}

					log.Printf("Unknown error: %v", reflect.TypeOf(err))
					return msgs, err
				}
			}
			//log.Printf("Read message: %v", msg)
			msgs = append(msgs, msg)
			msgCount++
		}
	}

}
