package middleware

import (
	"github.com/IBM/sarama"
	"gitlab.mobbtech.com/gozix/kafka/internal/client"
	"gitlab.mobbtech.com/gozix/kafka/kafkaapi"
	"go.uber.org/zap"
)

func NewZap(logger *zap.Logger) *Zap {
	return &Zap{
		logger: logger,
	}
}

type Zap struct {
	logger *zap.Logger
}

func (z Zap) Single() kafkaapi.MiddlewareFunc {
	return func(next client.Consume) client.Consume {
		return func(message *sarama.ConsumerMessage) (err error) {
			var log = z.logger.With(
				zap.String("topic", message.Topic),
				zap.Int64("offset", message.Offset),
				zap.Int32("partition", message.Partition))
			log.Debug("Start Consume message")
			err = next(message)
			if err != nil {
				log.Error("Error Consume message", zap.Error(err))
			}
			log.Debug("End Consume message")
			return err
		}
	}
}

func (z Zap) Batch() kafkaapi.BatchMiddlewareFunc {
	return func(next client.ConsumeBatch) client.ConsumeBatch {
		return func(messages []*sarama.ConsumerMessage) (err error) {
			var log = z.logger.With(
				zap.String("topic", messages[0].Topic),
				zap.Int64("offset start", messages[0].Offset),
				zap.Int64("offset end", messages[len(messages)-1].Offset),
				zap.Int32("partition", messages[0].Partition))
			log.Debug("Start Consume batch messages")
			err = next(messages)
			if err != nil {
				log.Error("Error Consume batch messages", zap.Error(err))
			}
			log.Debug("End Consume batch message")
			return err
		}
	}
}
