// Command consumer joins a consumer group and prints the partitions assigned to this process.
//
// Start two processes with the same group to watch a 6-partition topic split across them:
//
//	go run ./consumer -name consumer-1 -brokers 127.0.0.1:29092
//	go run ./consumer -name consumer-2 -brokers 127.0.0.1:29092
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"

	"github.com/IBM/sarama"

	"kafka-lab/constants"
	"kafka-lab/topic"
)

// consumerConfig is the runtime configuration for one member of the lab consumer group.
type consumerConfig struct {
	// name identifies this process in the logs.
	name string
	// brokers are the bootstrap servers used for the first connection.
	brokers []string
	// topic is the Kafka topic whose partitions are assigned to the group.
	topic string
	// groupID is the consumer group that shares the topic's partitions.
	groupID string
}

// groupHandler receives the partitions Sarama assigns to this consumer.
type groupHandler struct {
	// name identifies this process in the logs.
	name string
	// topic is the topic whose assignment is logged.
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

// parseConfig reads flags and falls back to the remote bootstrap server.
func parseConfig() (consumerConfig, error) {
	nameFlag := flag.String("name", "", "name of this consumer, for example consumer-1")
	brokersFlag := flag.String("brokers", constants.RemoteBootstrapServer, "comma-separated bootstrap servers")
	topicFlag := flag.String("topic", constants.DefaultTopic, "topic to read")
	groupFlag := flag.String("group", constants.ConsumerGroupID, "consumer group id")
	flag.Parse()

	if len(*nameFlag) == 0 {
		return consumerConfig{}, fmt.Errorf("name must not be empty")
	}
	if len(*topicFlag) == 0 {
		return consumerConfig{}, fmt.Errorf("topic must not be empty")
	}
	if len(*groupFlag) == 0 {
		return consumerConfig{}, fmt.Errorf("group must not be empty")
	}

	brokers := splitBrokers(*brokersFlag)
	if len(brokers) == 0 {
		return consumerConfig{}, fmt.Errorf("brokers must not be empty")
	}

	return consumerConfig{
		name:    *nameFlag,
		brokers: brokers,
		topic:   *topicFlag,
		groupID: *groupFlag,
	}, nil
}

// run joins the consumer group and reads only the partitions assigned to this member.
func run(ctx context.Context, cfg consumerConfig) error {
	err := topic.EnsurePartitions(ctx, cfg.brokers, cfg.topic, constants.TopicPartitionCount, constants.TopicReplicationFactor)
	if err != nil {
		return err
	}

	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.MaxVersion
	// Range gives each member a contiguous block of partitions.
	// With 6 partitions and 2 members, one receives 0-2 and the other receives 3-5.
	saramaConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRange(),
	}
	// OffsetOldest applies only when this group has no committed offset yet.
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

	group, err := sarama.NewConsumerGroup(cfg.brokers, cfg.groupID, saramaConfig)
	if err != nil {
		return fmt.Errorf("create consumer group: %w", err)
	}
	defer group.Close()

	log.Printf("%s joined group %s for topic %s", cfg.name, cfg.groupID, cfg.topic)

	handler := &groupHandler{
		name:  cfg.name,
		topic: cfg.topic,
	}
	for {
		err := group.Consume(ctx, []string{cfg.topic}, handler)
		if err != nil {
			if errors.Is(err, sarama.ErrClosedConsumerGroup) || errors.Is(err, context.Canceled) {
				log.Printf("%s stopped", cfg.name)
				return nil
			}
			return fmt.Errorf("consume: %w", err)
		}
		if ctx.Err() != nil {
			log.Printf("%s stopped", cfg.name)
			return nil
		}
	}
}

// Setup logs the partitions assigned to this member after each rebalance.
func (handler *groupHandler) Setup(session sarama.ConsumerGroupSession) error {
	log.Printf("%s assigned partitions %v", handler.name, partitionIDs(session.Claims()[handler.topic]))
	return nil
}

// Cleanup runs when this member gives up its partitions at the end of a generation.
func (handler *groupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim reads one assigned partition until the group session ends.
func (handler *groupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case <-session.Context().Done():
			return nil
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			log.Printf("%s read partition=%d offset=%d key=%s value=%s", handler.name, message.Partition, message.Offset, message.Key, message.Value)
			session.MarkMessage(message, "")
		}
	}
}

// partitionIDs returns the partition numbers from a group claim, in order.
func partitionIDs(partitions []int32) []int {
	ids := make([]int, 0, len(partitions))
	for _, partition := range partitions {
		ids = append(ids, int(partition))
	}
	sort.Ints(ids)
	return ids
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
