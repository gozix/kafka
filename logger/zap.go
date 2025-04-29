package logger

import "go.uber.org/zap"

func NewZapInternalLogger(logger *zap.Logger) InternalLogger {
	return ZapLoggRer{logger}
}

type ZapLoggRer struct {
	*zap.Logger
}

func (z ZapLoggRer) Named(name string) InternalLogger {
	z.Logger = z.Logger.Named(name)
	return z
}

func (z ZapLoggRer) InfoListen(msg string, topic, group string) {
	z.Info(msg, zap.String("topic", topic), zap.String("group", group))
}

func (z ZapLoggRer) ErrorListen(msg string, topic, group string, err error) {
	z.Error(msg, zap.String("topic", topic), zap.String("group", group), zap.Error(err))
}

func (z ZapLoggRer) ErrorConsume(msg string, topic string, partition int32, Err error) {
	z.Error(msg, zap.String("topic", topic), zap.Int32("partition", partition), zap.Error(Err))
}

func (z ZapLoggRer) ErrorBatchListen(msg string, topic, group string, partition int32, size int, Err error) {
	z.Error(msg, zap.String("topic", topic), zap.String("group", group),
		zap.Int32("partition", partition), zap.Int("size", size), zap.Error(Err))
}

func (z ZapLoggRer) ErrorBatchConsume(msg string, topic string, partition int32, size int, Err error) {
	z.Error(msg, zap.String("topic", topic), zap.Int32("partition", partition),
		zap.Int("size", size), zap.Error(Err))
}

func (z ZapLoggRer) WarnListen(msg string, topic, group string, Err error) {
	z.Warn(msg, zap.String("topic", topic), zap.String("group", group), zap.Error(Err))
}

func (z ZapLoggRer) DebugAddConsume(msg string, gen int32) {
	z.Debug(msg, zap.Int32("gen", gen))
}

func (z ZapLoggRer) DebugFlushConsume(msg string, gen int, size int32) {
	z.Debug(msg, zap.Int("gen", gen), zap.Int32("size", size))
}
