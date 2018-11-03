package main

import (
	"context"
	k "github.com/segmentio/kafka-go"
	"io"
	"log"
	"net"
	"strconv"
	"time"
)

func UnpartitionedReader(topic string) {

	brokers := []string{"localhost:9092"}

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
		conn, err := DialPartition(context.Background(), "tcp", k.DefaultDialer, p)
		if err != nil {
			log.Printf("Failed to dial leader: %v", err)
		}
		const safetyTimeout = 10 * time.Second
		deadline := time.Now().Add(safetyTimeout)
		conn.SetReadDeadline(deadline)
		msgs, err := ReadMessages(conn)
		if err != nil {
			log.Fatal("Failed to read messages: %v", err)
		}

		for _, m := range msgs {
			log.Printf("Key: %v - %v", string(m.Key), string(m.Value))
		}
	}
}

func ReadMessages(conn *k.Conn) ([]k.Message, error) {
	var msgs []k.Message
	for {
		batch := conn.ReadBatch(0, 100)

		msgCount := 0
		for {
			msg, err := batch.ReadMessage()
			if err == io.EOF {
				err := batch.Close()
				if err != nil {
					return nil, err
				}
				if msgCount == 0 {
					return msgs, nil
				} else {
					break
				}
			}
			if err != nil {
				return nil, err
			}
			msgs = append(msgs, msg)
			msgCount++
		}
	}

}

func DialPartition(ctx context.Context, network string, d *k.Dialer, p k.Partition) (*k.Conn, error) {
	address := net.JoinHostPort(p.Leader.Host, strconv.Itoa(p.Leader.Port))
	if r := d.Resolver; r != nil {
		addrs, err := r.LookupHost(ctx, p.Leader.Host)
		if err != nil {
			return nil, err
		}
		log.Printf("Looked up addrs: %v", addrs)
		if len(addrs) != 0 {
			address = addrs[0]
		} else {
			log.Fatalf("Empty list of addresses")
		}
		address, _ = splitHostPort(address)
		address = net.JoinHostPort(address, strconv.Itoa(p.Leader.Port))
	}

	conn, err := (&net.Dialer{
		LocalAddr:     d.LocalAddr,
		DualStack:     d.DualStack,
		FallbackDelay: d.FallbackDelay,
		KeepAlive:     d.KeepAlive,
	}).DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}

	return k.NewConnWith(conn, k.ConnConfig{
		ClientID:  d.ClientID,
		Topic:     p.Topic,
		Partition: p.ID,
	}), nil
}

func splitHostPort(s string) (host string, port string) {
	host, port, _ = net.SplitHostPort(s)
	if len(host) == 0 && len(port) == 0 {
		host = s
	}
	return
}
