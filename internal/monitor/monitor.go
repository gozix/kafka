package monitor

type Status string

const (
	StatusSuccess Status = "success"
	StatusFailed  Status = "failed"
)

type Monitor interface {
	WithConnection(name string) Monitor
	TopicMessagesProducedTotal(topic string, status Status)
	TopicMessagesConsumedTotal(topic string, count int, status Status)
	HandlingConsumingMessageSeconds(name string, duration float64, status Status)
}
