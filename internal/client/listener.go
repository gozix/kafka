package client

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type Listener interface {
	Listen(ctx context.Context, group, topic string, handler sarama.ConsumerGroupHandler) error
}

type listener struct {
	client sarama.Client
	logger *zap.Logger

	isListening atomic.Bool
}

func (l *listener) Listen(ctx context.Context, group, topic string, handler sarama.ConsumerGroupHandler) (err error) {
	if !l.isListening.CompareAndSwap(false, true) {
		return errors.New("already listening")
	}
	defer l.isListening.Store(false)

	if l.client.Closed() {
		return errors.New("client is closed")
	}

	// PLEASE NOTE: consumer groups can only re-use but not share clients.
	var consumerGroup sarama.ConsumerGroup
	if consumerGroup, err = sarama.NewConsumerGroupFromClient(group, l.client); err != nil {
		return err
	}

	defer func() {
		_ = consumerGroup.Close()
	}()

	wg, ctx := errgroup.WithContext(ctx)

	wg.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			default:
			}

			l.logger.Info("Listening... ",
				zap.String("topic", topic),
				zap.String("group", group),
			)
			if err := consumerGroup.Consume(ctx, []string{topic}, handler); err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					l.logger.Warn("Consumer closed the connection",
						zap.Error(err),
						zap.String("topic", topic),
						zap.String("group", group),
					)
					return nil
				}
				l.logger.Error("Consumer finished listening with error",
					zap.Error(err),
					zap.String("topic", topic),
					zap.String("group", group),
				)
			}
		}
	})

	return wg.Wait()
}
