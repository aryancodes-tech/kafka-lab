// Command producer writes a message per second to the lab topic.
// Run it on the laptop that hosts the Kafka container.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/IBM/sarama"

	"kafka-lab/constants"
	"kafka-lab/topic"
)

// producerConfig is the runtime configuration for the lab producer.
type producerConfig struct {
	// brokers are the bootstrap servers used for the first connection.
	brokers []string
	// topic is the Kafka topic messages are written to.
	topic string
}

func main() {
	cfg, err := parseConfig()
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg); err != nil {
		log.Fatal(err)
	}
}

// parseConfig reads flags and falls back to the local bootstrap server.
func parseConfig() (producerConfig, error) {
	brokersFlag := flag.String("brokers", constants.LocalBootstrapServer, "comma-separated bootstrap servers")
	topicFlag := flag.String("topic", constants.DefaultTopic, "topic to write")
	flag.Parse()

	if len(*topicFlag) == 0 {
		return producerConfig{}, fmt.Errorf("topic must not be empty")
	}

	brokers := splitBrokers(*brokersFlag)
	if len(brokers) == 0 {
		return producerConfig{}, fmt.Errorf("brokers must not be empty")
	}

	return producerConfig{
		brokers: brokers,
		topic:   *topicFlag,
	}, nil
}

// run writes one message per second until ctx is cancelled.
func run(ctx context.Context, cfg producerConfig) error {
	err := topic.EnsurePartitions(ctx, cfg.brokers, cfg.topic, constants.TopicPartitionCount, constants.TopicReplicationFactor)
	if err != nil {
		return err
	}

	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.MaxVersion
	// WaitForLocal waits for the leader. This lab broker has a single replica.
	saramaConfig.Producer.RequiredAcks = sarama.WaitForLocal
	saramaConfig.Producer.Return.Successes = true
	// RoundRobin spreads messages across all partitions so both consumers receive some.
	saramaConfig.Producer.Partitioner = sarama.NewRoundRobinPartitioner

	producer, err := sarama.NewSyncProducer(cfg.brokers, saramaConfig)
	if err != nil {
		return fmt.Errorf("create producer: %w", err)
	}
	defer producer.Close()

	log.Printf("producing to topic %s via %s", cfg.topic, strings.Join(cfg.brokers, ","))

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	sequence := 0
	for {
		select {
		case <-ctx.Done():
			log.Printf("producer stopped")
			return nil
		case <-ticker.C:
			sequence++
			message := &sarama.ProducerMessage{
				Topic: cfg.topic,
				Key:   sarama.StringEncoder(fmt.Sprintf("msg-%d", sequence)),
				Value: sarama.StringEncoder(fmt.Sprintf("hello from this laptop #%d at %s", sequence, time.Now().Format(time.RFC3339))),
			}
			partition, offset, err := producer.SendMessage(message)
			if err != nil {
				if ctx.Err() != nil {
					log.Printf("producer stopped")
					return nil
				}
				return fmt.Errorf("write message %d: %w", sequence, err)
			}
			log.Printf("sent partition=%d offset=%d key=%s value=%s", partition, offset, message.Key, message.Value)
		}
	}
}

// splitBrokers splits a comma-separated bootstrap list and drops blank entries.
func splitBrokers(raw string) []string {
	parts := strings.Split(raw, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		broker := strings.TrimSpace(part)
		if len(broker) == 0 {
			continue
		}
		brokers = append(brokers, broker)
	}
	return brokers
}
