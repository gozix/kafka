package client

import (
	"errors"
	"gitlab.mobbtech.com/gozix/kafka/logger"
	"gitlab.mobbtech.com/gozix/kafka/monitor"
	"time"

	"github.com/IBM/sarama"
)

type (
	Consume      func(*sarama.ConsumerMessage) error
	ConsumeBatch func([]*sarama.ConsumerMessage) error
)

type ConsumerWrapper struct {
	consume Consume
	monitor monitor.Monitor
	logger  logger.InternalLogger
}

func NewConsumerWrapper(consume Consume, monitor monitor.Monitor, logger logger.InternalLogger) *ConsumerWrapper {
	return &ConsumerWrapper{consume: consume, logger: logger, monitor: monitor}
}

func (s *ConsumerWrapper) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (s *ConsumerWrapper) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (s *ConsumerWrapper) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case m, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			var start = time.Now()
			if err := s.consume(m); err != nil {
				if !errors.Is(err, ErrMessageRejected) {
					time.Sleep(100 * time.Millisecond)
					s.monitor.TopicMessagesConsumedTotal(m.Topic, 1, monitor.StatusFailed)
					s.monitor.HandlingConsumingMessageSeconds(m.Topic, time.Since(start).Seconds(), monitor.StatusFailed)
					return err
				}
			}

			session.MarkMessage(m, "")
			s.monitor.TopicMessagesConsumedTotal(m.Topic, 1, monitor.StatusSuccess)
			s.monitor.HandlingConsumingMessageSeconds(m.Topic, time.Since(start).Seconds(), monitor.StatusSuccess)

		case <-session.Context().Done():
			return nil
		}
	}
}

type BatchConsumerWrapper struct {
	consume ConsumeBatch
	monitor monitor.Monitor
	logger  logger.InternalLogger
	size    int
}

func NewBatchConsumerWrapper(consume ConsumeBatch, size int, monitor monitor.Monitor, logger logger.InternalLogger) *BatchConsumerWrapper {
	return &BatchConsumerWrapper{consume: consume, size: size, logger: logger, monitor: monitor}
}

func (s *BatchConsumerWrapper) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (s *BatchConsumerWrapper) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (s *BatchConsumerWrapper) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	var (
		delay = 10 * time.Second
		timer = time.NewTimer(delay)
		batch = make([]*sarama.ConsumerMessage, 0, s.size)
	)

	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				if len(batch) == 0 {
					return nil
				}
			} else {
				batch = append(batch, message)
				s.logger.DebugAddConsume("Add item to batch", session.GenerationID())
				if len(batch) < s.size {
					continue
				}
			}

		case <-timer.C:
			timer.Reset(delay)
			s.logger.DebugFlushConsume("Flushing batch by timeout", len(batch), session.GenerationID())

		case <-session.Context().Done():
			return nil
		}

		if len(batch) == 0 {
			continue
		}

		var (
			start   = time.Now()
			message = batch[len(batch)-1]
		)
		if err := s.consume(batch); err != nil {
			if !errors.Is(err, ErrMessageRejected) {
				time.Sleep(100 * time.Millisecond)
				s.monitor.TopicMessagesConsumedTotal(message.Topic, len(batch), monitor.StatusFailed)
				s.monitor.HandlingConsumingMessageSeconds(message.Topic, time.Since(start).Seconds(), monitor.StatusFailed)
				return err
			}
		}

		session.MarkOffset(message.Topic, message.Partition, message.Offset+1, "")
		s.monitor.TopicMessagesConsumedTotal(message.Topic, len(batch), monitor.StatusSuccess)
		s.monitor.HandlingConsumingMessageSeconds(message.Topic, time.Since(start).Seconds(), "")

		batch = batch[:0]
	}
}
