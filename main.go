package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"gopkg.in/alecthomas/kingpin.v2"
)

type config struct {
	Consumer struct {
		Group            string        `json:"group"`
		Topics           string        `json:"topics"`
		Brokers          string        `json:"brokers"`
		OffsetInitial    string        `json:"initial_offset"`
		SessionTimeout   time.Duration `json:"session_timeout"`
		RebalanceTimeout time.Duration `json:"rebalance_timeout"`
	} `json:"consumer"`
	Producer struct {
		Brokers       string        `json:"brokers"`
		Rewrite       string        `json:"rewrite"`
		Compression   string        `json:"compression"`
		Partitioner   string        `json:"partitioner"`
		BatchSize     int           `json:"batch_size"`
		FlushInterval time.Duration `json:"flush_interval"`
	} `json:"producer"`
}

func (c *config) String() string {
	d, _ := json.MarshalIndent(c, "", "  ")
	return string(d)
}

var cfg = new(config)

func init() {
	// consumer options
	{
		kingpin.Flag("group", "The name of the consumer group").
			Envar("GROUP").
			Default("mirrormaker").StringVar(&cfg.Consumer.Group)
		kingpin.Flag("topics", "The comma-separated list of topics to consume").
			Envar("TOPICS").
			Default("").StringVar(&cfg.Consumer.Topics)
		kingpin.Flag("consume-brokers", "A comma-separated broker connection string").
			Envar("CONSUME_BROKERS").
			Default("").StringVar(&cfg.Consumer.Brokers)
		kingpin.Flag("offset-initial", "The initial offset method (oldest, newest)").
			Envar("INITIAL_OFFSET").
			Default("oldest").StringVar(&cfg.Consumer.OffsetInitial)
		kingpin.Flag("session-timeout", "Consumer session timeout").
			Envar("SESSION_TIMEOUT").
			Default("1m").DurationVar(&cfg.Consumer.SessionTimeout)
		kingpin.Flag("rebalance-timeout", "Consumer rebalance timeout").
			Envar("REBALANCE_TIMEOUT").
			Default("1m").DurationVar(&cfg.Consumer.RebalanceTimeout)
	}
	// producer options
	{
		kingpin.Flag("produce-brokers", "A comma-separated broker connection string").
			Envar("PRODUCE_BROKERS").
			Default("").StringVar(&cfg.Producer.Brokers)
		kingpin.Flag("rewrite", `The rewrite rules (--rewrite="from-topic-name->to-topic-name,from-topic-name2->to-topic-name2")`).
			Envar("REWRITE").
			Default("").StringVar(&cfg.Producer.Rewrite)
		kingpin.Flag("compression", "The compression to be used by the producer (none, gzip, snappy, lz4)").
			Envar("COMPRESSION").
			Default("none").StringVar(&cfg.Producer.Compression)
		kingpin.Flag("partitioner", "Partitioner to use (random, keyhash, roundrobin)").
			Envar("PARTITIONER").
			Default("roundrobin").StringVar(&cfg.Producer.Partitioner)
		kingpin.Flag("batch-size", "The maximum count of messages in batch request").
			Envar("BATCH_SIZE").
			Default("10000").IntVar(&cfg.Producer.BatchSize)
		kingpin.Flag("flush-interval", "The flush interval in millisecond").
			Envar("FLUSH_INTERVAL").
			Default("1s").DurationVar(&cfg.Producer.FlushInterval)
	}

	kingpin.Parse()

	log.Printf("Running with config:\n%s\n", cfg.String())
}

func main() {
	var mirrormaker = &Mirrormaker{
		topics:     strings.Split(cfg.Consumer.Topics, ","),
		ticker:     time.NewTicker(cfg.Producer.FlushInterval),
		buffer:     make(chan *sarama.ConsumerMessage, cfg.Producer.BatchSize/2),
		batchSize:  cfg.Producer.BatchSize,
		rewriteMap: make(map[string]string),
	}

	for _, str := range strings.Split(cfg.Producer.Rewrite, ",") {
		if parts := strings.Split(str, "->"); len(parts) == 2 {
			log.Printf("Rewriting topic: %s -> %s\n", parts[0], parts[1])
			mirrormaker.rewriteMap[parts[0]] = parts[1]
		}
	}

	// setup kafka consumer
	{
		consumerConfig := sarama.NewConfig()
		consumerConfig.Consumer.Group.Session.Timeout = cfg.Consumer.SessionTimeout
		consumerConfig.Consumer.Group.Rebalance.Timeout = cfg.Consumer.RebalanceTimeout
		consumerConfig.Consumer.Group.Rebalance.Retry.Max = 16
		consumerConfig.Consumer.Group.Rebalance.Retry.Backoff = time.Second * 3

		switch cfg.Consumer.OffsetInitial {
		case "newest":
			consumerConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
		default:
			consumerConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
		}

		consumer, err := sarama.NewConsumerGroup(
			strings.Split(cfg.Consumer.Brokers, ","),
			cfg.Consumer.Group,
			consumerConfig,
		)
		if err != nil {
			log.Fatalf("Failed to initialize kafka consumer: %s", err.Error())
		}

		mirrormaker.consumer = consumer
	}

	// setup kafka producer
	{
		producerConfig := sarama.NewConfig()
		producerConfig.Producer.Timeout = time.Minute
		producerConfig.Producer.Return.Successes = true
		producerConfig.Producer.RequiredAcks = sarama.WaitForLocal

		switch cfg.Producer.Compression {
		case "lz4":
			producerConfig.Producer.Compression = sarama.CompressionLZ4
		case "gzip":
			producerConfig.Producer.Compression = sarama.CompressionGZIP
		case "snappy":
			producerConfig.Producer.Compression = sarama.CompressionSnappy
		default:
			producerConfig.Producer.Compression = sarama.CompressionNone
		}

		switch cfg.Producer.Partitioner {
		case "random":
			producerConfig.Producer.Partitioner = sarama.NewRandomPartitioner
		case "keyhash":
			producerConfig.Producer.Partitioner = sarama.NewHashPartitioner
		case "roundrobin":
			producerConfig.Producer.Partitioner = sarama.NewRoundRobinPartitioner
		default:
			producerConfig.Producer.Partitioner = sarama.NewRoundRobinPartitioner
		}

		producer, err := sarama.NewSyncProducer(
			strings.Split(cfg.Producer.Brokers, ","),
			producerConfig,
		)
		if err != nil {
			log.Fatalf("Failed to initialize kafka producer: %s", err.Error())
		}

		mirrormaker.producer = producer
	}

	// run replicator in separate goroutine
	go func() {
		if err := mirrormaker.Run(); err != nil {
			log.Fatalf("Worker exited with error: %s\n", err.Error())
		}
	}()

	// listen for exit signals
	sys := make(chan os.Signal, 1)
	signal.Notify(sys, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	shutdown(<-sys, mirrormaker)
}

func shutdown(_ os.Signal, mm *Mirrormaker) {
	log.Println("Shutting down...")
	mm.Stop()
	os.Exit(0)
}

// Mirrormaker replicates kafka topics
type Mirrormaker struct {
	ctx         context.Context
	ctxCancel   context.CancelFunc
	topics      []string
	consumer    sarama.ConsumerGroup
	buffer      chan *sarama.ConsumerMessage
	ticker      *time.Ticker
	producer    sarama.SyncProducer
	cache       []*sarama.ConsumerMessage
	cacheOffset int
	rewriteMap  map[string]string
	batchSize   int
}

func (m *Mirrormaker) Validate() error {
	if len(m.topics) == 0 {
		return errors.New("no topics provided for replication")
	}
	return nil
}

// Run starts replication process
func (m *Mirrormaker) Run() error {
	if err := m.Validate(); err != nil {
		return err
	}

	// create context to be used for running goroutines
	m.ctx, m.ctxCancel = context.WithCancel(context.Background())

	// flush data periodically
	go func() {
		for {
			select {
			case <-m.ticker.C:
				m.flush()
			case <-m.ctx.Done():
				log.Println("Stopping flushing data...")
				return
			}
		}
	}()

	// log all consumer errors from kafka
	go func() {
		for {
			select {
			case err := <-m.consumer.Errors():
				log.Printf("Kafka consumer error: %s\n", err.Error())
			case <-m.ctx.Done():
				log.Println("Stopping streaming kafka errors...")
				return
			}
		}
	}()

	// start consuming kafka topics
	return m.consume()
}

// Stop stops consumer and producer
func (m *Mirrormaker) Stop() {
	// cancel all processes
	m.ctxCancel()

	// close consumer
	log.Println("Stopping consumer...")
	if err := m.consumer.Close(); err != nil {
		log.Printf("Consumer close error: %s\n", err.Error())
	}

	// read all buffered messages before exit
	log.Printf("Flushing data in buffer (%d) ...\n", len(m.buffer))
	for len(m.buffer) > 0 {
		m.flush()
	}

	// close buffer channel
	close(m.buffer)

	// close producer
	log.Println("Stopping producer...")
	if err := m.producer.Close(); err != nil {
		log.Printf("Producer close error: %s\n", err.Error())
	}

	log.Println("Replication gracefully stopped")
}

// consume starts consuming messages from kafka
func (m *Mirrormaker) consume() (err error) {
	for {
		log.Println("Starting new kafka group consumer")
		if err = m.consumer.Consume(
			m.ctx, m.topics, m,
		); err != nil {
			break // system error
		}
		if m.ctx.Err() != nil {
			break // was canceled by Stop()
		}
	}
	return
}

// flush messages from buffer to end kafka
func (m *Mirrormaker) flush() {

	batch := make([]*sarama.ProducerMessage, 0, m.batchSize)

	for msg := m.next(); msg != nil; msg = m.next() {
		batch = append(batch, &sarama.ProducerMessage{
			Partition: -1,
			Topic:     m.rewrite(msg.Topic),
			Value:     sarama.ByteEncoder(msg.Value),
			Key:       sarama.ByteEncoder(msg.Key),
		})
		if len(batch) >= m.batchSize {
			break
		}
	}

	if len(batch) > 0 {
		if err := m.producer.SendMessages(batch); err != nil {
			log.Printf("Flush error: %s", err)
			return
		}
		log.Printf("Flushed %d messages", len(batch))
	}

}

// next reads and returns message from buffer
func (m *Mirrormaker) next() *sarama.ConsumerMessage {
	select {
	case msg := <-m.buffer:
		return msg
	default:
		return nil
	}
}

func (m *Mirrormaker) rewrite(topic string) string {
	if n, ok := m.rewriteMap[topic]; ok {
		return n
	}
	return topic
}

// sarama.ConsumerGroupHandler interface methods

func (m *Mirrormaker) Setup(s sarama.ConsumerGroupSession) error {
	return nil
}

func (m *Mirrormaker) Cleanup(s sarama.ConsumerGroupSession) error {
	return nil
}

func (m *Mirrormaker) ConsumeClaim(s sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		m.buffer <- message
		s.MarkMessage(message, "")
	}
	return nil
}
