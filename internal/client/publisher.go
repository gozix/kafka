package client

import (
	"errors"
	"gitlab.mobbtech.com/gozix/kafka/monitor"

	"github.com/IBM/sarama"
)

type Publisher interface {
	Publish(msg *sarama.ProducerMessage) error
}

type publisher struct {
	producer sarama.SyncProducer
	monitor  monitor.Monitor
}

func (p *publisher) Publish(msg *sarama.ProducerMessage) error {
	if msg == nil {
		return nil
	}

	if p.producer == nil {
		return errors.New("no connection resources available")
	}

	_, _, err := p.producer.SendMessage(msg)
	var status = monitor.StatusSuccess
	if err != nil {
		status = monitor.StatusFailed
	}
	p.monitor.TopicMessagesProducedTotal(msg.Topic, status)
	return err
}

func (p *publisher) Close() error {
	if p.producer != nil {
		return p.producer.Close()
	}
	return nil
}
