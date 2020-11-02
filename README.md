# Kafka replicator

Replicates data from local kafka to main kafka cluster.  
It uses sarama under the hood with group consumer and sync producer.  

## Build

`make build` to build binary file for linux.  
`make image` to build image `hasansino/kafka-replicator:latest`

## Usage

```bash
usage: tsctl [<flags>]

Flags:
  --help                      Show context-sensitive help (also try --help-long and --help-man).
  --group="mirrormaker"       The name of the consumer group
  --topics=""                 The comma-separated list of topics to consume
  --rewrite=""                The rewrite rules (--rewrite="from-topic-name->to-topic-name,from-topic-name2->to-topic-name2")
  --consume-brokers=""        A comma-separated broker connection string (localhost:9092, localhost:9092)
  --produce-brokers=""        A comma-separated broker connection string (localhost:9092, localhost:9092)
  --compression="none"        The compression codec to be used by the producer (none, gzip, snappy, lz4)
  --offset-initial="oldest"   The initial offset method (oldest, newest)
  --batch-size=10000          The maximum count of messages in batch request
  --flush-interval=1s         The flush interval in millisecond
  --session-timeout=1m        Kafka session timeout
  --partitioner="roundrobin"  partitioner to use (random, keyhash, roundrobin)
```