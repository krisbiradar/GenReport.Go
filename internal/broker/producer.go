package broker

import (
	"context"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
)

// Producer publishes messages to RabbitMQ queues.
type Producer struct {
	channel *amqp.Channel
	logger  zerolog.Logger
}

// NewProducer creates a producer from an existing broker.
func NewProducer(b *Broker) *Producer {
	return &Producer{
		channel: b.Channel(),
		logger:  b.logger,
	}
}

// Publish sends a message to the specified queue (topic).
// The queue is declared as durable if it doesn't already exist.
func (p *Producer) Publish(topic string, payload []byte) error {
	_, err := p.channel.QueueDeclare(
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = p.channel.PublishWithContext(ctx,
		"",    // default exchange
		topic, // routing key = queue name
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         payload,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish to %q: %w", topic, err)
	}

	p.logger.Debug().
		Str("topic", topic).
		Int("bytes", len(payload)).
		Msg("published message")

	return nil
}
