package client

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/IBM/sarama"
	"golang.org/x/sync/errgroup"

	"github.com/gozix/kafka/logger"
)

type Listener interface {
	Listen(ctx context.Context, group, topic string, handler sarama.ConsumerGroupHandler) error
	Lag(topic, group string) (int64, error)
}

type listener struct {
	client sarama.Client
	logger logger.InternalLogger

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

			l.logger.InfoListen("Listening... ", topic, group)
			if err := consumerGroup.Consume(ctx, []string{topic}, handler); err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					l.logger.WarnListen("Consumer closed the connection", topic, group, err)
					return nil
				}
				l.logger.ErrorListen("Consumer finished listening with error", topic, group, err)
			}
		}
	})

	return wg.Wait()
}

func (l *listener) Lag(topic, group string) (int64, error) {
	partitions, err := l.client.Partitions(topic)
	if err != nil {
		return -1, err
	}
	var lag int64
	for _, partition := range partitions {
		latestOffset, err := l.client.GetOffset(topic, partition, sarama.OffsetNewest)
		if err != nil {
			continue
		}
		coordinator, err := l.client.Coordinator(group)
		if err != nil {
			continue
		}
		offsetFetchReq := &sarama.OffsetFetchRequest{Version: 1}
		offsetFetchReq.AddPartition(topic, partition)
		offsetFetchResp, err := coordinator.FetchOffset(offsetFetchReq)
		if err != nil {
			continue
		}
		block := offsetFetchResp.GetBlock(topic, partition)
		if block == nil || block.Offset < 0 {
			continue
		}
		lag += latestOffset - block.Offset
	}
	return lag, nil
}
