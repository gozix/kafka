package monitor

type nopMonitor struct{}

func (n *nopMonitor) WithConnection(name string) Monitor { return n }

func (n *nopMonitor) TopicMessagesProducedTotal(topic string, status Status) {}

func (n *nopMonitor) TopicMessagesConsumedTotal(_ string, _ int, _ Status) {}

func (n *nopMonitor) HandlingConsumingMessageSeconds(_ string, _n float64, _ Status) {}

func NewNop() Monitor {
	return &nopMonitor{}
}
