package main

import (
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

func main() {

	topic := "comments"
	worker, err := connnectConsumer([]string{"localhost:29092"})
	if err != nil {
		log.Println(err)
		panic(err)
	}

	consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		log.Println(err)
		panic(err)
	}

	slog.Info("Consumer up and running!...")
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	msgCount := 0

	doneCh := make(chan struct{})

	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				log.Println(err)
			case msg := <-consumer.Messages():
				msgCount++
				log.Printf("Received Count %d, Message: %s Topic: %s\n", msgCount, string(msg.Value), msg.Topic)
			case <-sigchan:
				slog.Info("Interrupt is detected")
				doneCh <- struct{}{}

			}
		}
	}()

	<-doneCh
	slog.Info("Processed", msgCount, "messages")
	if err := worker.Close(); err != nil {
		log.Println(err)
	}
}

func connnectConsumer(brokersUrl []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer, err := sarama.NewConsumer(brokersUrl, config)
	if err != nil {
		return nil, err
	}
	return consumer, nil
}
