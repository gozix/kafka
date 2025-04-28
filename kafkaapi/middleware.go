package kafkaapi

import "gitlab.mobbtech.com/gozix/kafka/internal/client"

type (
	MiddlewareFunc      func(next client.Consume) client.Consume
	BatchMiddlewareFunc func(next client.ConsumeBatch) client.ConsumeBatch

	Middleware interface {
		Single() MiddlewareFunc
		Batch() BatchMiddlewareFunc
	}

	Middlewares []Middleware
)

func (mvs Middlewares) WrapSingle(h client.Consume) client.Consume {
	for i := len(mvs) - 1; i >= 0; i-- {
		h = mvs[i].Single()(h)
	}

	return h
}

func (mvs Middlewares) WrapBatch(h client.ConsumeBatch) client.ConsumeBatch {
	for i := len(mvs) - 1; i >= 0; i-- {
		h = mvs[i].Batch()(h)
	}

	return h
}
