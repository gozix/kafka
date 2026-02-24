// Package command contains dynamic Kafka listener management logic.
package command

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/gozix/di"
	"github.com/gozix/kafka/internal/client"
	"github.com/gozix/kafka/internal/modifier"
	"github.com/gozix/kafka/kafkaapi"
	"github.com/gozix/kafka/logger"
	"github.com/gozix/kafka/monitor"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"
)

const (
	// TagListenerDynamic is a tag to mark dynamic kafka listener's.
	TagListenerDynamic = "kafka.listener_dynamic"
	// TagBatchListenerDynamic is a tag to mark dynamic kafka batch listener's.
	TagBatchListenerDynamic = "kafka.batch_listener_dynamic"
)

type listenerEntry struct {
	cancel       context.CancelFunc
	done         <-chan struct{} // closed when Listen goroutine exits
	listener     client.Listener
	subscription kafkaapi.Subscription
}

func syncDynamicListeners(
	ctx context.Context,
	mu *sync.Mutex,
	log logger.InternalLogger,
	listenerFactory kafkaapi.ListenerDynamic,
	getTopics func() ([]string, error),
	listenersMap map[string]listenerEntry,
	createWrappedListener func(sub kafkaapi.Subscription) (client.Listener, error),
	createHandler func(listener kafkaapi.Listener, sub kafkaapi.Subscription) sarama.ConsumerGroupHandler,
) {
	topics, err := getTopics()
	if err != nil {
		return
	}
	mu.Lock()

	existing := make(map[string]struct{}, len(listenersMap))
	for topic := range listenersMap {
		existing[topic] = struct{}{}
	}

	for _, topic := range topics {
		if _, ok := listenersMap[topic]; !ok {
			childCtx, cancel := context.WithCancel(ctx)
			apiListener := listenerFactory.NewListenerForTopic(topic)
			sub := apiListener.Subscription()
			sub.Topic = topic
			apiListener.SetConfiguration(client.ListenerDefaultConfig())
			wrappedListener, err := createWrappedListener(sub)
			if err != nil {
				cancel()
				continue
			}
			handler := createHandler(apiListener, sub)
			done := make(chan struct{})
			go func(wrappedListener client.Listener, childCtx context.Context, group, topic string, handler sarama.ConsumerGroupHandler) {
				defer close(done)
				if err := wrappedListener.Listen(childCtx, group, topic, handler); err != nil {
					log.ErrorListen("dynamic listener error", topic, group, err)
				}
			}(wrappedListener, childCtx, sub.Group, sub.Topic, handler)
			listenersMap[topic] = listenerEntry{
				cancel:       cancel,
				done:         done,
				listener:     wrappedListener,
				subscription: sub,
			}
		}
		delete(existing, topic)
	}

	// collect entries to stop (lag == 0) while still under lock
	var toStop []listenerEntry
	for topic := range existing {
		entry := listenersMap[topic]
		lag, _ := entry.listener.Lag(topic, entry.subscription.Group)
		if lag == 0 {
			entry.cancel()
			delete(listenersMap, topic)
			toStop = append(toStop, entry)
		}
	}
	mu.Unlock()

	// wait and close outside the lock to avoid blocking the mutex
	for _, entry := range toStop {
		<-entry.done
		_ = entry.listener.Close()
	}
}

func syncDynamicBatchListeners(
	ctx context.Context,
	mu *sync.Mutex,
	log logger.InternalLogger,
	listenerFactory kafkaapi.BatchListenerDynamic,
	getTopics func() ([]string, error),
	listenersMap map[string]listenerEntry,
	createWrappedListener func(sub kafkaapi.Subscription) (client.Listener, error),
	createHandler func(listener kafkaapi.BatchListener, sub kafkaapi.Subscription) sarama.ConsumerGroupHandler,
) {
	topics, err := getTopics()
	if err != nil {
		return
	}
	mu.Lock()

	existing := make(map[string]struct{}, len(listenersMap))
	for topic := range listenersMap {
		existing[topic] = struct{}{}
	}

	for _, topic := range topics {
		if _, ok := listenersMap[topic]; !ok {
			childCtx, cancel := context.WithCancel(ctx)
			apiListener := listenerFactory.NewBatchListenerForTopic(topic)
			sub := apiListener.Subscription()
			sub.Topic = topic
			apiListener.SetConfiguration(client.ListenerDefaultConfig())
			wrappedListener, err := createWrappedListener(sub)
			if err != nil {
				cancel()
				continue
			}
			handler := createHandler(apiListener, sub)
			done := make(chan struct{})
			go func(wrappedListener client.Listener, childCtx context.Context, group, topic string, handler sarama.ConsumerGroupHandler) {
				defer close(done)
				if err := wrappedListener.Listen(childCtx, group, topic, handler); err != nil {
					log.ErrorListen("dynamic batch listener error", topic, group, err)
				}
			}(wrappedListener, childCtx, sub.Group, sub.Topic, handler)
			listenersMap[topic] = listenerEntry{
				cancel:       cancel,
				done:         done,
				listener:     wrappedListener,
				subscription: sub,
			}
		}
		delete(existing, topic)
	}

	// collect entries to stop (lag == 0) while still under lock
	var toStop []listenerEntry
	for topic := range existing {
		entry := listenersMap[topic]
		lag, _ := entry.listener.Lag(topic, entry.subscription.Group)
		if lag == 0 {
			entry.cancel()
			delete(listenersMap, topic)
			toStop = append(toStop, entry)
		}
	}
	mu.Unlock()

	// wait and close outside the lock to avoid blocking the mutex
	for _, entry := range toStop {
		<-entry.done
		_ = entry.listener.Close()
	}
}

// groupListenersByConnection groups ListenerDynamic by connection name.
func groupListenersByConnection(listeners []kafkaapi.ListenerDynamic) map[string][]kafkaapi.ListenerDynamic {
	result := make(map[string][]kafkaapi.ListenerDynamic)
	for _, l := range listeners {
		conn := l.Subscription().Connection
		result[conn] = append(result[conn], l)
	}
	return result
}

// groupBatchListenersByConnection groups BatchListenerDynamic by connection name.
func groupBatchListenersByConnection(listeners []kafkaapi.BatchListenerDynamic) map[string][]kafkaapi.BatchListenerDynamic {
	result := make(map[string][]kafkaapi.BatchListenerDynamic)
	for _, l := range listeners {
		conn := l.Subscription().Connection
		result[conn] = append(result[conn], l)
	}
	return result
}

// runDynamicListenerGroup launches a goroutine to manage a group of dynamic listeners for a specific connection.
func runDynamicListenerGroup(
	ctx context.Context,
	mu *sync.Mutex,
	wg *errgroup.Group,
	cfg *viper.Viper,
	factory client.Factory,
	log logger.InternalLogger,
	conn string,
	listeners any,
	middlewares kafkaapi.Middlewares,
	monitor monitor.Monitor,
) {
	wg.Go(func() error {
		listenersMapsByIndex := make(map[int]map[string]listenerEntry)

		interval := cfg.GetDuration("kafka." + conn + ".listener_dynamic.poll_interval")
		if interval == 0 {
			interval = 10 * time.Second
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		init := make(chan struct{}, 1)
		init <- struct{}{}

		for {
			select {
			case <-ctx.Done():
				// cancel all listeners and wait for them to finish
				mu.Lock()
				for _, listenersMap := range listenersMapsByIndex {
					for _, entry := range listenersMap {
						entry.cancel()
					}
				}
				mu.Unlock()

				for _, listenersMap := range listenersMapsByIndex {
					for _, entry := range listenersMap {
						<-entry.done
						_ = entry.listener.Close()
					}
				}
				return nil

			case <-init:
			case <-ticker.C:
			}

			switch ls := listeners.(type) {
			case []kafkaapi.ListenerDynamic:
				for index, listenerFactory := range ls {
					listenersMap, ok := listenersMapsByIndex[index]
					if !ok {
						listenersMap = make(map[string]listenerEntry)
						listenersMapsByIndex[index] = listenersMap
					}
					syncDynamicListeners(
						ctx, mu, log, listenerFactory,
						listenerFactory.TopicListCheck,
						listenersMap,
						func(sub kafkaapi.Subscription) (client.Listener, error) {
							return factory.NewListener(
								client.WithConnectName(sub.Connection),
								client.WithConfig(client.ListenerDefaultConfig()),
							)
						},
						func(listener kafkaapi.Listener, sub kafkaapi.Subscription) sarama.ConsumerGroupHandler {
							return consumerGroupFactory(
								middlewares.WrapSingle(listener.Handle),
								monitor, log, sub.Topic)(sub)
						},
					)
				}
			case []kafkaapi.BatchListenerDynamic:
				for index, listenerFactory := range ls {
					listenersMap, ok := listenersMapsByIndex[index]
					if !ok {
						listenersMap = make(map[string]listenerEntry)
						listenersMapsByIndex[index] = listenersMap
					}
					syncDynamicBatchListeners(
						ctx, mu, log, listenerFactory,
						listenerFactory.TopicListCheck,
						listenersMap,
						func(sub kafkaapi.Subscription) (client.Listener, error) {
							return factory.NewListener(
								client.WithConnectName(sub.Connection),
								client.WithConfig(client.ListenerDefaultConfig()),
							)
						},
						func(listener kafkaapi.BatchListener, sub kafkaapi.Subscription) sarama.ConsumerGroupHandler {
							return batchConsumerGroupFactory(
								middlewares.WrapBatch(listener.Handle),
								monitor, log, sub.Topic)(sub)
						},
					)
				}
			}
		}
	})
}

// NewKafkaListenerDynamic creates a command for dynamic Kafka listener's management.
func NewKafkaListenerDynamic(cnt di.Container) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dynamic-listener",
		Short: "Run dynamic Kafka listeners (auto-manage by topic list)",
		RunE: func(cmd *cobra.Command, args []string) error {
			var modListener, modBatchListener *modifier.Modifier
			var err error
			if modListener, err = modifier.NewModifier(TagListenerDynamic, args); err != nil {
				return fmt.Errorf("unable create listener glob modifier : %w", err)
			}
			if modBatchListener, err = modifier.NewModifier(TagBatchListenerDynamic, args); err != nil {
				return fmt.Errorf("unable create listener glob modifier : %w", err)
			}

			return cnt.Call(func(
				ctx context.Context,
				log logger.InternalLogger,
				cfg *viper.Viper,
				middlewares kafkaapi.Middlewares,
				monitor monitor.Monitor,
				dynamicListeners []kafkaapi.ListenerDynamic,
				dynamicBatchListeners []kafkaapi.BatchListenerDynamic,
			) error {
				var (
					mu       sync.Mutex
					wg, ctx2 = errgroup.WithContext(ctx)
				)

				factory, err := client.NewFactory(cfg, log)
				if err != nil {
					return err
				}

				byConn := groupListenersByConnection(dynamicListeners)
				byBatchConn := groupBatchListenersByConnection(dynamicBatchListeners)

				for conn, listeners := range byConn {
					runDynamicListenerGroup(ctx2, &mu, wg, cfg, factory, log, conn, listeners, middlewares, monitor)
				}
				for conn, listeners := range byBatchConn {
					runDynamicListenerGroup(ctx2, &mu, wg, cfg, factory, log, conn, listeners, middlewares, monitor)
				}

				return wg.Wait()
			},
				di.Constraint(5, di.Optional(true), modListener.Modifier()),
				di.Constraint(6, di.Optional(true), modBatchListener.Modifier()))
		},
	}
	return cmd
}
