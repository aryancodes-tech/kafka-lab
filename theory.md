# Kafka Docker Compose Configuration — Detailed Explanation

This is a **Docker Compose setup for a single-node Apache Kafka cluster running in KRaft mode**, plus a **Kafka UI** container.

> **Important:** There is one typo in the original configuration:
>
> ```yaml
> PLAINTEXT_EXTE RNAL://0.0.0.0:29092
> ```
>
> should be:
>
> ```yaml
> PLAINTEXT_EXTERNAL://0.0.0.0:29092
> ```
>
> The space in `EXTE RNAL` will break the listener configuration.

---

# 1. Big Picture: What Are You Creating?

Your Docker Compose creates **two containers**:

```text
┌───────────────────────────────────────────────────────────┐
│                    Docker Compose                         │
│                                                           │
│   ┌──────────────────────┐      ┌─────────────────────┐  │
│   │       Kafka          │      │     Kafka UI        │  │
│   │                      │      │                     │  │
│   │ Broker               │◄─────│ kafka:9092          │  │
│   │ Controller           │      │                     │  │
│   │                      │      │ Web UI :8080        │  │
│   └──────────┬───────────┘      └─────────────────────┘  │
│              │                                            │
└──────────────┼────────────────────────────────────────────┘
               │
         Docker network
               │
        ┌──────┴──────┐
        │ Your Mac    │
        │             │
        │ :29092      │
        │ :8080       │
        └─────────────┘
```

Kafka has **two different networking audiences** in your configuration:

### Other Docker containers

They connect using:

```text
kafka:9092
```

For example:

```text
Kafka UI ───────► kafka:9092
```

### Applications running on your Mac

They connect using:

```text
localhost:29092
```

or through the advertised external address:

```text
host.docker.internal:29092
```

That's why you have two listeners.

---

# 2. `services:`

```yaml
services:
```

This is the root section where you define the containers that Docker Compose should create.

You're defining two services:

```yaml
services:
  kafka:
  kafka-ui:
```

Think of a Compose file as saying:

> "Here are the containers I want, how they should be configured, and how they should communicate."

---

# 3. Kafka Service

```yaml
kafka:
```

This defines a service named `kafka`.

Inside Docker's Compose network, this service gets a DNS hostname:

```text
kafka
```

That's why you can later write:

```text
kafka:9092
```

instead of needing an IP address.

---

# 4. Kafka Docker Image

```yaml
image: apache/kafka:latest
```

This tells Docker:

> Pull the Apache Kafka image and use it to create the Kafka container.

The image contains Kafka and everything necessary to run it.

`latest` means:

> Use the image tagged `latest`.

For learning this is convenient, but for production you'd generally pin a specific Kafka version, for example:

```yaml
image: apache/kafka:4.x.x
```

because `latest` can change over time.

---

# 5. Container Name

```yaml
container_name: kafka-lab
```

Normally Docker Compose generates a name based on the project and service.

You're explicitly saying:

```text
kafka-lab
```

So:

```bash
docker ps
```

will show something like:

```text
kafka-lab
```

You can then do:

```bash
docker logs kafka-lab
```

or:

```bash
docker exec -it kafka-lab bash
```

---

# 6. Ports

```yaml
ports:
  - "9092:9092"
  - "29092:29092"
```

This is extremely important.

The syntax is:

```text
HOST_PORT:CONTAINER_PORT
```

So:

```yaml
"9092:9092"
```

means:

```text
Your Mac :9092
       ↓
Kafka container :9092
```

And:

```yaml
"29092:29092"
```

means:

```text
Your Mac :29092
       ↓
Kafka container :29092
```

You have two Kafka ports because you have two broker listeners.

---

# 7. Why Two Kafka Ports?

This is one of the most confusing parts of Kafka.

Your Kafka container needs to support:

### Docker → Kafka

For example:

```text
Kafka UI
   │
   │ kafka:9092
   ▼
 Kafka
```

### Host → Kafka

For example, your Go application running directly on your Mac:

```text
Go application
     │
     │ localhost:29092
     ▼
 Docker Kafka
```

These are two different networking environments.

---

# 8. `environment:`

```yaml
environment:
```

Everything underneath this is passed to Kafka as an environment variable.

The Apache Kafka Docker image uses these variables to configure Kafka.

Conceptually:

```text
docker-compose.yml
       ↓
environment variables
       ↓
Kafka startup script
       ↓
Kafka configuration
```

---

# KRaft Configuration

Modern Kafka can run without ZooKeeper using **KRaft**.

Older Kafka architecture:

```text
Kafka
  │
  └── ZooKeeper
```

Modern architecture:

```text
Kafka
  │
  └── KRaft controller
```

KRaft essentially uses Kafka itself to manage cluster metadata.

---

# 9. `KAFKA_NODE_ID`

```yaml
KAFKA_NODE_ID: 1
```

This gives this Kafka server a unique ID:

```text
1
```

Think of it like the server's identity card.

If you had:

```text
Kafka server A → node ID 1
Kafka server B → node ID 2
Kafka server C → node ID 3
```

Kafka can distinguish them.

Because you're running only one Kafka server:

```text
node ID = 1
```

is perfectly fine.

---

# 10. `KAFKA_PROCESS_ROLES`

```yaml
KAFKA_PROCESS_ROLES: broker,controller
```

This is one of the most important lines.

You're telling Kafka:

> This Kafka process should act as both a BROKER and a CONTROLLER.

Kafka has two conceptual responsibilities.

## Broker

The broker handles actual Kafka data:

```text
Producer
   │
   ▼
Kafka Broker
   │
   ├── Topic
   ├── Partitions
   └── Messages
```

It handles:

* Producing messages
* Consuming messages
* Storing messages
* Serving partitions

## Controller

The controller manages cluster metadata.

For example:

* Which broker owns which partition?
* Partition leadership
* Broker membership
* Cluster metadata
* Topic/partition metadata

So your single Kafka process is doing both:

```text
Kafka process
     │
     ├── Broker
     │
     └── Controller
```

For a local development environment, this is normal.

In larger production clusters, broker and controller roles can be separated.

---

# 11. `KAFKA_CONTROLLER_QUORUM_VOTERS`

```yaml
KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
```

This tells Kafka:

> Which nodes participate in the KRaft controller quorum?

The format is:

```text
node_id@hostname:port
```

So:

```text
1@kafka:9093
```

means:

```text
Node ID: 1
Hostname: kafka
Port: 9093
```

Your cluster therefore has:

```text
Controller
    │
    ▼
kafka:9093
```

Notice something important:

**9093 is not your client Kafka port.**

It's the controller communication port.

---

# 12. Why `9093`?

You've configured:

```yaml
CONTROLLER://0.0.0.0:9093
```

So Kafka listens for controller communication on:

```text
9093
```

You don't expose this port to your Mac because external applications don't need to talk directly to the KRaft controller.

It's internal Kafka infrastructure.

---

# Listeners

Now we get into the most important Kafka networking concept.

---

# 13. `KAFKA_LISTENERS`

You have:

```yaml
KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,PLAINTEXT_EXTERNAL://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093
```

This tells Kafka:

> On which interfaces and ports should Kafka actually listen?

You have **three listeners**:

```text
PLAINTEXT
    ↓
0.0.0.0:9092

PLAINTEXT_EXTERNAL
    ↓
0.0.0.0:29092

CONTROLLER
    ↓
0.0.0.0:9093
```

Let's break this down.

---

# 14. `PLAINTEXT://0.0.0.0:9092`

```text
PLAINTEXT://0.0.0.0:9092
```

Means:

```text
Listener name = PLAINTEXT
Protocol      = PLAINTEXT
Bind address  = 0.0.0.0
Port          = 9092
```

`0.0.0.0` means:

> Listen on all network interfaces inside the container.

So Kafka is effectively saying:

```text
Listen for connections on port 9092
on all container network interfaces.
```

This listener is intended for internal Docker communication.

So Kafka UI can connect using:

```text
kafka:9092
```

---

# 15. `PLAINTEXT_EXTERNAL`

You intended:

```yaml
PLAINTEXT_EXTERNAL://0.0.0.0:29092
```

This is another listener.

Its purpose is:

> Provide a Kafka endpoint intended for clients outside the Docker network.

For example, your Go program running directly on your Mac:

```go
kafka.NewWriter(kafka.WriterConfig{
    Brokers: []string{"localhost:29092"},
})
```

would use the external listener.

---

# 16. `CONTROLLER`

```text
CONTROLLER://0.0.0.0:9093
```

This listener is for KRaft controller communication.

It's not intended for:

* Go applications
* Kafka UI
* Kafka producers
* Kafka consumers

Instead:

```text
Kafka controller
       ↕
KRaft controller communication
```

---

# 17. Advertised Listeners

Now comes the subtle but **extremely important** distinction between:

```text
listeners
```

and:

```text
advertised.listeners
```

You have:

```yaml
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_EXTERNAL://host.docker.internal:29092
```

### `KAFKA_LISTENERS`

means:

> "Where should Kafka listen?"

### `KAFKA_ADVERTISED_LISTENERS`

means:

> "When a client asks me where to connect, what address should I tell it?"

This distinction is fundamental to Kafka.

---

# 18. Why Does Kafka Need Advertised Listeners?

Imagine your Go application connects to Kafka.

It initially connects to:

```text
localhost:29092
```

Kafka says:

> Okay, here's the broker information.

It might tell the client:

```text
Connect to kafka:9092
```

But your Go application is running on your Mac.

Your Mac doesn't know what:

```text
kafka
```

means.

That's a Docker DNS hostname.

So the connection fails.

Instead, Kafka needs to tell external clients:

```text
host.docker.internal:29092
```

which your Mac can reach.

---

# 19. Internal Advertised Listener

```text
PLAINTEXT://kafka:9092
```

This tells Docker-network clients:

> Reach me using `kafka:9092`.

For example:

```text
Kafka UI
   │
   │ kafka:9092
   ▼
Kafka
```

Docker's internal DNS resolves:

```text
kafka
```

to the Kafka container.

---

# 20. External Advertised Listener

```text
PLAINTEXT_EXTERNAL://host.docker.internal:29092
```

This tells external clients:

> Reach me using `host.docker.internal:29092`.

`host.docker.internal` is a special hostname Docker provides so containers can refer to the host machine.

On Docker Desktop, this is particularly useful.

Your setup therefore looks like:

```text
                     Docker
┌───────────────────────────────────────────┐
│                                           │
│  Kafka UI                                 │
│     │                                     │
│     │ kafka:9092                          │
│     ▼                                     │
│  ┌──────────────┐                         │
│  │    Kafka     │                         │
│  │              │                         │
│  │ :9092        │◄── internal             │
│  │ :29092       │◄── external             │
│  │ :9093        │◄── controller           │
│  └──────────────┘                         │
│                                           │
└───────────────────┬───────────────────────┘
                    │
              Docker Desktop
                    │
             host.docker.internal
                    │
                    ▼
                 Your Mac
```

---

# 21. `KAFKA_CONTROLLER_LISTENER_NAMES`

```yaml
KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
```

You're telling Kafka:

> The listener named `CONTROLLER` should be used for KRaft controller communication.

You defined:

```text
CONTROLLER://0.0.0.0:9093
```

So Kafka knows:

```text
KRaft controller traffic
        ↓
CONTROLLER
        ↓
9093
```

---

# 22. `KAFKA_INTER_BROKER_LISTENER_NAME`

```yaml
KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
```

This specifies which listener Kafka brokers should use when communicating with each other.

If you eventually had:

```text
Broker 1
Broker 2
Broker 3
```

they would use:

```text
PLAINTEXT
```

for broker-to-broker communication.

Since you only have one broker right now, this doesn't have much practical effect, but Kafka still needs the configuration.

---

# 23. Replication Factor

```yaml
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

Kafka internally stores consumer group offsets in a special topic:

```text
__consumer_offsets
```

This configuration says:

```text
Replication factor = 1
```

Meaning:

```text
1 copy
```

Why?

Because you only have one Kafka broker.

You can't have:

```text
replication factor = 3
```

with only:

```text
1 broker
```

---

# 24. Transaction State Replication

```yaml
KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
```

Kafka maintains internal transaction state.

This says:

```text
Transaction state replication = 1
```

Again, because you have one broker.

---

# 25. Transaction State Minimum ISR

```yaml
KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
```

ISR means:

> **In-Sync Replicas**

If replication factor were 3:

```text
Broker 1 ✓
Broker 2 ✓
Broker 3 ✓
```

all could be in the ISR.

You're saying Kafka needs at least:

```text
1
```

in-sync replica.

With a single-node cluster, that's necessary.

---

# 26. Listener Security Protocol Map

```yaml
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT
```

This is basically a mapping:

```text
Listener name          Protocol

PLAINTEXT          →   PLAINTEXT
PLAINTEXT_EXTERNAL →   PLAINTEXT
CONTROLLER         →   PLAINTEXT
```

The first value is the listener name.

The second value is the Kafka security protocol.

For example:

```text
PLAINTEXT_EXTERNAL:PLAINTEXT
```

means:

> The listener called `PLAINTEXT_EXTERNAL` uses the Kafka PLAINTEXT protocol.

---

# 27. What Does PLAINTEXT Actually Mean?

It means:

```text
No TLS encryption
No authentication
```

So this is suitable for a local development environment.

For example:

```text
Producer
   │
   │ plaintext
   ▼
Kafka
```

In production you'd typically consider authentication and encryption, such as SASL/TLS.

But for learning Kafka locally:

```text
PLAINTEXT
```

keeps things much simpler.

---

# 28. Kafka Storage

```yaml
KAFKA_LOG_DIRS: /tmp/kraft-combined-logs
```

Kafka stores messages on disk.

This configuration says:

> Store Kafka's log data under this directory inside the container.

Kafka doesn't think of messages simply as:

```text
message 1
message 2
message 3
```

Instead, Kafka stores records in append-only log files.

For example:

```text
/tmp/kraft-combined-logs/

    my-topic-0/
        00000000000000000000.log
        ...
```

Kafka partitions are represented by these log directories/files.

---

# 29. Important: Your Data Is Not Persistent

Notice that you haven't configured:

```yaml
volumes:
```

Therefore your Kafka data exists inside the container filesystem.

If you remove the container, you can lose your Kafka data.

For a learning environment this is usually fine.

For persistent local Kafka you'd do something like:

```yaml
volumes:
  - kafka-data:/tmp/kraft-combined-logs
```

and then:

```yaml
volumes:
  kafka-data:
```

---

# 30. Kafka UI Service

Now:

```yaml
kafka-ui:
```

This creates your second container.

Its purpose is to provide a web interface for Kafka.

Instead of doing everything with CLI commands:

```bash
kafka-topics
kafka-console-producer
kafka-console-consumer
```

you can open:

```text
http://localhost:8080
```

and visually inspect Kafka.

---

# 31. Kafka UI Image

```yaml
image: provectuslabs/kafka-ui:latest
```

This downloads the Kafka UI Docker image from the registry.

It contains the Kafka UI application.

Again, `latest` means the latest image tag.

---

# 32. Kafka UI Container Name

```yaml
container_name: kafka-ui
```

Explicitly names the container:

```text
kafka-ui
```

So:

```bash
docker logs kafka-ui
```

works.

---

# 33. `depends_on`

```yaml
depends_on:
  - kafka
```

This tells Compose:

> Start the Kafka service before starting Kafka UI.

So Compose roughly does:

```text
Start Kafka
    ↓
Start Kafka UI
```

However, an important nuance:

`depends_on` does **not necessarily mean**:

> Kafka is fully ready to accept connections.

It mainly establishes startup ordering.

Kafka might still be initializing when Kafka UI starts.

A healthcheck is often used when you need true readiness semantics.

---

# 34. Kafka UI Port

```yaml
ports:
  - "8080:8080"
```

Again:

```text
HOST:CONTAINER
```

So:

```text
Your Mac
localhost:8080
      │
      ▼
Kafka UI container
      │
    :8080
```

Therefore:

```text
http://localhost:8080
```

opens Kafka UI.

---

# 35. Kafka UI Environment Variables

```yaml
environment:
```

You're now configuring Kafka UI.

---

# 36. Cluster Name

```yaml
KAFKA_CLUSTERS_0_NAME: local-kafka
```

Kafka UI can manage multiple Kafka clusters.

For example:

```text
Cluster 1 → local
Cluster 2 → staging
Cluster 3 → production
```

The `_0_` means:

```text
Cluster index 0
```

You're naming your cluster:

```text
local-kafka
```

This is just the friendly name shown in the UI.

It doesn't have to match Kafka's internal cluster ID.

---

# 37. Bootstrap Servers

```yaml
KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092
```

This tells Kafka UI:

> To connect to the Kafka cluster, initially connect to `kafka:9092`.

Notice:

```text
kafka:9092
```

not:

```text
localhost:9092
```

Why?

Because Kafka UI itself is running **inside Docker**.

Its network looks like:

```text
Kafka UI container
       │
       │ Docker network
       │
       ▼
Kafka container
```

Docker's internal DNS resolves:

```text
kafka
```

to the Kafka container.

So:

```text
kafka:9092
```

is correct.

---

# 38. Complete Networking Picture

Your entire setup is essentially:

```text
                           YOUR MAC
                ┌──────────────────────────┐
                │                          │
                │   Go Application         │
                │        │                 │
                │        │ localhost:29092 │
                │        ▼                 │
                │                          │
                │   Browser                │
                │        │                 │
                │        │ localhost:8080  │
                │        ▼                 │
                └────────┼─────────────────┘
                         │
                    Docker Desktop
                         │
        ┌────────────────┴───────────────────┐
        │                                    │
        │         Docker Network             │
        │                                    │
        │   ┌──────────────┐                 │
        │   │ Kafka UI     │                 │
        │   │              │                 │
        │   │ kafka:9092 ──┼───────┐         │
        │   └──────────────┘       │         │
        │                          ▼         │
        │                   ┌─────────────┐  │
        │                   │   Kafka     │  │
        │                   │             │  │
        │                   │ :9092       │  │
        │                   │ :29092      │  │
        │                   │ :9093       │  │
        │                   └─────────────┘  │
        │                                    │
        └────────────────────────────────────┘
```

---

# 39. The Three Kafka Listeners Summarized

This is probably the most important thing to understand from your entire file.

| Listener             |  Port | Who Uses It?      | Purpose                  |
| -------------------- | ----: | ----------------- | ------------------------ |
| `PLAINTEXT`          |  9092 | Docker containers | Internal Kafka traffic   |
| `PLAINTEXT_EXTERNAL` | 29092 | Host applications | External/client access   |
| `CONTROLLER`         |  9093 | KRaft             | Controller communication |

And their addresses:

```text
Docker:

kafka:9092
```

```text
Host:

host.docker.internal:29092
```

```text
KRaft:

kafka:9093
```

---

# 40. Why `localhost:9092` Can Be Confusing

A very common Kafka Docker mistake is doing:

```yaml
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
```

This might work for something running on your host, but not for another Docker container.

For Kafka UI:

```text
Kafka UI
   │
   └── localhost:9092
```

would mean:

> Connect to port 9092 of the Kafka UI container itself.

Not Kafka.

That's why you use:

```text
kafka:9092
```

for Docker clients.

---

# 41. One More Subtle Point: `host.docker.internal`

Your configuration advertises:

```text
host.docker.internal:29092
```

That's useful when a client is connecting through the host/Docker Desktop boundary.

Depending on where the client runs, you may instead use:

```text
localhost:29092
```

as the bootstrap address from your Mac.

For example:

```go
brokers := []string{
    "localhost:29092",
}
```

Kafka can then advertise the externally reachable address appropriately.

The important idea is that **the address Kafka advertises must be reachable from the client's network environment**.

---

# 42. What Happens When You Run `docker compose up`?

Conceptually:

## Step 1

Docker creates the Kafka container.

```text
kafka-lab
```

## Step 2

Kafka starts.

It sees:

```text
NODE_ID = 1
ROLES = broker,controller
```

## Step 3

Kafka starts its listeners:

```text
9092 → internal broker
29092 → external broker
9093 → controller
```

## Step 4

Kafka starts its KRaft controller.

```text
Node 1
   │
   └── Controller
```

## Step 5

Kafka starts its broker.

```text
Node 1
   │
   └── Broker
```

## Step 6

Kafka UI starts.

It reads:

```text
kafka:9092
```

## Step 7

Kafka UI connects to Kafka.

```text
Kafka UI
   │
   │ kafka:9092
   ▼
Kafka
```

## Step 8

You open:

```text
localhost:8080
```

and see your Kafka cluster.

---

# 43. The Whole Configuration in Plain English

Your Compose file basically says:

> **Create a Kafka container called `kafka-lab`.**
>
> Run Kafka in modern KRaft mode rather than ZooKeeper.
>
> Make this Kafka process both a broker and a controller.
>
> Give it node ID 1.
>
> Use port 9093 internally for KRaft controller communication.
>
> Allow Docker containers to access Kafka through `kafka:9092`.
>
> Allow applications outside the Docker network to access Kafka through port `29092`.
>
> Tell Kafka to advertise `kafka:9092` to internal Docker clients and `host.docker.internal:29092` to external clients.
>
> Store Kafka's data under `/tmp/kraft-combined-logs`.
>
> Since this is a single-node cluster, use replication factors of 1.
>
> Then start a Kafka UI container.
>
> Have Kafka UI connect to `kafka:9092`.
>
> Expose Kafka UI on `localhost:8080`.

---

# 44. Corrected Complete Configuration

Your current configuration has the `EXTERNAL` typo. I'd write it as:

```yaml
services:
  kafka:
    image: apache/kafka:latest
    container_name: kafka-lab

    ports:
      - "9092:9092"
      - "29092:29092"

    environment:
      # KRaft
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093

      # Listeners
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,PLAINTEXT_EXTERNAL://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093

      # What addresses Kafka tells clients to use
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_EXTERNAL://host.docker.internal:29092

      # Listener configuration
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT

      # Single broker
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1

      # Security protocol mapping
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT

      # Kafka data
      KAFKA_LOG_DIRS: /tmp/kraft-combined-logs

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui

    depends_on:
      - kafka

    ports:
      - "8080:8080"

    environment:
      KAFKA_CLUSTERS_0_NAME: local-kafka
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092
```

---

# 45. The Most Important Concept to Remember

If you're learning Kafka, remember this distinction:

```text
┌───────────────────────────┐
│       LISTENERS           │
│                           │
│ "Where do I listen?"      │
│                           │
│ 9092                      │
│ 29092                     │
│ 9093                      │
└───────────────────────────┘
             │
             ▼
┌───────────────────────────┐
│   ADVERTISED LISTENERS    │
│                           │
│ "What address do I tell   │
│  clients to connect to?"  │
│                           │
│ kafka:9092                │
│ host.docker.internal:29092│
└───────────────────────────┘
```

Once this clicks, Kafka's Docker networking becomes dramatically less confusing.
