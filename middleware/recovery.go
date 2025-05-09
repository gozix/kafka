package middleware

import (
	"fmt"
	"runtime/debug"

	"github.com/IBM/sarama"

	"github.com/gozix/kafka/internal/client"
	"github.com/gozix/kafka/kafkaapi"
)

func NewRecovery() *Recovery {
	return &Recovery{}
}

type Recovery struct{}

func (m Recovery) Single() kafkaapi.MiddlewareFunc {
	return func(next client.Consume) client.Consume {
		return func(message *sarama.ConsumerMessage) (err error) {
			defer func() {
				if recovered := recover(); recovered != nil {
					err = fmt.Errorf("panic: %v: stacktrace: %s", recovered, debug.Stack())
				}
			}()

			return next(message)
		}
	}
}

func (m Recovery) Batch() kafkaapi.BatchMiddlewareFunc {
	return func(next client.ConsumeBatch) client.ConsumeBatch {
		return func(messages []*sarama.ConsumerMessage) (err error) {
			defer func() {
				if recovered := recover(); recovered != nil {
					err = fmt.Errorf("panic: %v: stacktrace: %s", recovered, debug.Stack())
				}
			}()

			return next(messages)
		}
	}
}
