package broker

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
)

// JobHandler is the function signature that workers must implement.
// It receives the raw JSON payload and returns an error if processing fails.
type JobHandler func(payload []byte) error

// Consumer consumes messages from RabbitMQ queues.
type Consumer struct {
	channel *amqp.Channel
	logger  zerolog.Logger
}

// NewConsumer creates a consumer from an existing broker.
func NewConsumer(b *Broker) *Consumer {
	return &Consumer{
		channel: b.Channel(),
		logger:  b.logger,
	}
}

// StartWorker declares the queue and spawns a goroutine that consumes messages
// one by one, calling handler for each. On handler success the message is acked;
// on failure it is nacked with requeue=false (message goes to dead-letter or is dropped).
// The goroutine exits when the done channel is closed.
func (c *Consumer) StartWorker(topic string, handler JobHandler, done <-chan struct{}) error {
	_, err := c.channel.QueueDeclare(
		topic, // queue name
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue %q: %w", topic, err)
	}

	// Prefetch 1 message at a time — process sequentially within this topic.
	if err := c.channel.Qos(1, 0, false); err != nil {
		return fmt.Errorf("failed to set QoS on %q: %w", topic, err)
	}

	msgs, err := c.channel.Consume(
		topic,                  // queue
		"worker-"+topic,        // consumer tag
		false,                  // auto-ack (false = manual ack)
		false,                  // exclusive
		false,                  // no-local
		false,                  // no-wait
		nil,                    // args
	)
	if err != nil {
		return fmt.Errorf("failed to start consuming %q: %w", topic, err)
	}

	c.logger.Info().Str("topic", topic).Msg("worker started")

	go func() {
		for {
			select {
			case <-done:
				c.logger.Info().Str("topic", topic).Msg("worker stopping")
				return
			case msg, ok := <-msgs:
				if !ok {
					c.logger.Warn().Str("topic", topic).Msg("message channel closed, worker exiting")
					return
				}

				c.logger.Debug().
					Str("topic", topic).
					Int("bytes", len(msg.Body)).
					Msg("received message")

				if err := handler(msg.Body); err != nil {
					c.logger.Error().
						Err(err).
						Str("topic", topic).
						Msg("job failed, nacking message")
					_ = msg.Nack(false, false) // no requeue
				} else {
					_ = msg.Ack(false)
				}
			}
		}
	}()

	return nil
}
