package logger

type InternalLogger interface {
	Named(name string) InternalLogger
	InfoListen(msg string, topic, group string)
	ErrorListen(msg string, topic, group string, err error)
	ErrorConsume(msg string, topic string, partition int32, Err error)
	ErrorBatchListen(msg string, topic, group string, partition int32, size int, Err error)
	ErrorBatchConsume(msg string, topic string, partition int32, size int, Err error)
	WarnListen(msg string, topic, group string, Err error)
	DebugAddConsume(msg string, gen int32)
	DebugFlushConsume(msg string, gen int, size int32)
}

func NewNopLogger() InternalLogger { return &NopLogger{} }

type NopLogger struct{}

func (l NopLogger) InfoListen(string, string, string)                          {}
func (l NopLogger) ErrorListen(string, string, string, error)                  {}
func (l NopLogger) ErrorConsume(string, string, int32, error)                  {}
func (l NopLogger) ErrorBatchListen(string, string, string, int32, int, error) {}
func (l NopLogger) ErrorBatchConsume(string, string, int32, int, error)        {}
func (l NopLogger) WarnListen(string, string, string, error)                   {}
func (l NopLogger) DebugAddConsume(string, int32)                              {}
func (l NopLogger) DebugFlushConsume(string, int, int32)                       {}
func (l NopLogger) Named(string) InternalLogger                                { return l }
