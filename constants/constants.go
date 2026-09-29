// Package constants holds the Kafka addresses and names shared by the lab producer and consumer.
package constants

const (
	// DefaultTopic is the topic the producer writes and the consumer reads.
	DefaultTopic = "lab-events"

	// LocalBootstrapServer is the external listener as seen from this laptop.
	// The producer, running next to the Kafka container, uses it for the first connection.
	// 127.0.0.1 avoids localhost resolving to IPv6, which Docker Desktop does not publish.
	LocalBootstrapServer = "127.0.0.1:29092"

	// RemoteBootstrapServer is the external listener as seen from another machine on the LAN.
	// It must match KAFKA_ADVERTISED_HOST in .env, plus the published PLAINTEXT_EXTERNAL port.
	RemoteBootstrapServer = "192.168.1.8:29092"

	// ConsumerGroupID is the consumer group the lab consumer joins.
	// Two processes using this same id share the topic's partitions.
	ConsumerGroupID = "lab-consumer"

	// TopicPartitionCount is the number of partitions created for DefaultTopic.
	// Two consumers in one group each receive a share of these partitions.
	TopicPartitionCount = 6

	// TopicReplicationFactor is the replica count for DefaultTopic.
	// The single-broker lab can store one replica.
	TopicReplicationFactor = 1
)
