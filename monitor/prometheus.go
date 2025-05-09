package monitor

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type prometheusMonitor struct {
	connection                      string
	topicMessagesConsumedTotal      *prometheus.CounterVec
	topicMessagesProducedTotal      *prometheus.CounterVec
	handlingConsumingMessageSeconds *prometheus.HistogramVec
}

func NewPrometheusMonitor(register prometheus.Registerer) Monitor {
	var factory = promauto.With(register)

	return &prometheusMonitor{
		topicMessagesProducedTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "kafka_topic_messages_produced_total",
			Help: "Count of produced messages",
		}, []string{"connection", "topic", "status"}),
		topicMessagesConsumedTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "kafka_topic_messages_consumed_total",
			Help: "Count of consumed messages",
		}, []string{"connection", "topic", "status"}),
		handlingConsumingMessageSeconds: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name: "kafka_handling_consuming_message_seconds",
			Help: "Count of consumed messages",
		}, []string{"connection", "topic", "status"}),
	}
}

func (p *prometheusMonitor) WithConnection(name string) Monitor {
	return &prometheusMonitor{
		connection:                      name,
		topicMessagesProducedTotal:      p.topicMessagesProducedTotal,
		topicMessagesConsumedTotal:      p.topicMessagesConsumedTotal,
		handlingConsumingMessageSeconds: p.handlingConsumingMessageSeconds,
	}
}

func (p *prometheusMonitor) TopicMessagesProducedTotal(topic string, status Status) {
	p.topicMessagesProducedTotal.WithLabelValues(p.connection, topic, string(status)).Inc()
}

func (p *prometheusMonitor) HandlingConsumingMessageSeconds(name string, duration float64, status Status) {
	p.handlingConsumingMessageSeconds.WithLabelValues(p.connection, name, string(status)).Observe(duration)
}

func (p *prometheusMonitor) TopicMessagesConsumedTotal(topic string, count int, status Status) {
	p.topicMessagesConsumedTotal.WithLabelValues(p.connection, topic, string(status)).Add(float64(count))
}
