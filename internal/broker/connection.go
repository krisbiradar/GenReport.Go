package broker

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
)

// Broker holds the RabbitMQ connection and channel.
type Broker struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	logger  zerolog.Logger
}

// Connect opens a RabbitMQ connection and channel.
func Connect(url string, logger zerolog.Logger) (*Broker, error) {
	if url == "" {
		return nil, fmt.Errorf("RABBITMQ_URL is empty")
	}

	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open RabbitMQ channel: %w", err)
	}

	logger.Info().Msg("connected to RabbitMQ")
	return &Broker{conn: conn, channel: ch, logger: logger}, nil
}

// Channel returns the underlying AMQP channel for creating producers/consumers.
func (b *Broker) Channel() *amqp.Channel {
	return b.channel
}

// Close gracefully shuts down the channel and connection.
func (b *Broker) Close() {
	if b.channel != nil {
		if err := b.channel.Close(); err != nil {
			b.logger.Error().Err(err).Msg("failed to close RabbitMQ channel")
		}
	}
	if b.conn != nil {
		if err := b.conn.Close(); err != nil {
			b.logger.Error().Err(err).Msg("failed to close RabbitMQ connection")
		}
	}
	b.logger.Info().Msg("RabbitMQ connection closed")
}
