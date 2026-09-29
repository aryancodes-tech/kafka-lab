// Package topic creates the lab topic and raises its partition count.
package topic

import (
	"context"
	"errors"
	"fmt"

	"github.com/IBM/sarama"
)

// EnsurePartitions creates topicName with partitionCount partitions.
// When the topic already exists, it raises the partition count up to partitionCount.
func EnsurePartitions(ctx context.Context, brokers []string, topicName string, partitionCount int, replicationFactor int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(topicName) == 0 {
		return fmt.Errorf("topic must not be empty")
	}
	if len(brokers) == 0 {
		return fmt.Errorf("brokers must not be empty")
	}

	config := sarama.NewConfig()
	config.Version = sarama.MaxVersion

	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		return fmt.Errorf("create cluster admin: %w", err)
	}
	defer admin.Close()

	detail := &sarama.TopicDetail{
		// NumPartitions is the partition count requested for a new topic.
		NumPartitions: int32(partitionCount),
		// ReplicationFactor is the number of replicas stored for each partition.
		ReplicationFactor: int16(replicationFactor),
	}
	err = admin.CreateTopic(topicName, detail, false)
	if err == nil {
		return nil
	}
	if !errors.Is(err, sarama.ErrTopicAlreadyExists) {
		return fmt.Errorf("create topic %s: %w", topicName, err)
	}

	err = admin.CreatePartitions(topicName, int32(partitionCount), nil, false)
	if err == nil || errors.Is(err, sarama.ErrInvalidPartitions) {
		return nil
	}
	return fmt.Errorf("expand topic %s: %w", topicName, err)
}
