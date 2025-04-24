// Package command contains cli command definitions.
package command

import (
	"context"
	"errors"
	"fmt"
	"gitlab.mobbtech.com/gozix/kafka/kafkaapi"

	"github.com/IBM/sarama"
	"github.com/gozix/di"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"gitlab.mobbtech.com/gozix/kafka/internal/client"
	"gitlab.mobbtech.com/gozix/kafka/internal/modifier"
	"gitlab.mobbtech.com/gozix/kafka/internal/monitor"
)

const (
	// TagListener is a tag to mark kafka listener's.
	TagListener = "kafka.listener"

	// TagBatchListener is a tag to mark kafka batch listener's.
	TagBatchListener = "kafka.batch_listener"

	// TagListenerFactory is a tag to mark kafka listener factories.
	TagListenerFactory = "kafka.listener_factory"

	// TagBatchListenerFactory is a tag to mark kafka listener factories.
	TagBatchListenerFactory = "kafka.listener_batch_factory"

	listenerPrefix = "kafka_listener_"
)

type (
	createHandler func(subscription kafkaapi.Subscription) sarama.ConsumerGroupHandler
)

// NewKafkaListener is a command constructor.
func NewKafkaListener(cnt di.Container) *cobra.Command {
	var (
		runE = func(cmd *cobra.Command, args []string) (err error) {
			var modListener *modifier.Modifier
			if modListener, err = modifier.NewModifier(TagListener, args); err != nil {
				return fmt.Errorf("unable create listener glob modifier : %w", err)
			}

			var modBatchListener *modifier.Modifier
			if modBatchListener, err = modifier.NewModifier(TagBatchListener, args); err != nil {
				return fmt.Errorf("unable create listener glob modifier : %w", err)
			}

			var modListenerFactory *modifier.Modifier
			if modListenerFactory, err = modifier.NewModifier(TagListenerFactory, args); err != nil {
				return fmt.Errorf("unable create listener glob modifier : %w", err)
			}

			var modBatchListenerFactory *modifier.Modifier
			if modBatchListenerFactory, err = modifier.NewModifier(TagBatchListenerFactory, args); err != nil {
				return fmt.Errorf("unable create listener glob modifier : %w", err)
			}

			var runE = func(
				ctx context.Context,
				logger *zap.Logger,
				cfg *viper.Viper,
				singleListeners []kafkaapi.Listener,
				batchListeners []kafkaapi.BatchListener,
				singleListenersFactory []kafkaapi.ListenerFactory,
				batchListenersFactory []kafkaapi.BatchListenerFactory,
				monitor monitor.Monitor,
			) error {
				// validate
				if len(singleListeners) == 0 && len(batchListeners) == 0 &&
					len(singleListenersFactory) == 0 && len(batchListenersFactory) == 0 {
					return errors.New("can't find any listener")
				}

				var (
					factory client.Factory
					wg      *errgroup.Group
				)
				wg, ctx = errgroup.WithContext(ctx)

				factory, err = client.NewFactory(cfg, logger)
				if err != nil {
					return err
				}

				var createWrappedListener = func(
					listener kafkaapi.ListenerSettings,
					createHandler createHandler) error {
					var subs = listener.Subscription()
					err = subs.Normalize()
					if err != nil {
						return err
					}
					var listenerCfg = client.ListenerDefaultConfig()
					listener.SetConfiguration(listenerCfg)
					for i := 0; i < subs.Count; i++ {
						var wrappedListener, errCreate = factory.NewListener(
							client.WithConnectName(subs.Connection),
							client.WithConfig(listenerCfg))
						if errCreate != nil {
							return errCreate
						}
						var handler = createHandler(subs)
						wg.Go(func() error {
							return wrappedListener.Listen(ctx, subs.Group, subs.Topic, handler)
						})
					}
					return nil
				}
				// listener
				for index, listener := range singleListeners {
					var name = modListener.Name(index)
					err = createWrappedListener(listener, consumerGroupFactory(listener.Handle, monitor, logger, name))
					if err != nil {
						return err
					}
				}
				// batch listener
				for index, listener := range batchListeners {
					var name = modBatchListener.Name(index)
					err = createWrappedListener(listener, batchConsumerGroupFactory(listener.Handle, monitor, logger, name))
					if err != nil {
						return err
					}
				}
				// listener factory
				for index, listenerFactory := range singleListenersFactory {
					var name = modListenerFactory.Name(index)
					var generatedListeners, errCreate = listenerFactory()
					if errCreate != nil {
						return errCreate
					}
					for _, listener := range generatedListeners {
						err = createWrappedListener(listener, consumerGroupFactory(listener.Handle, monitor, logger, name))
						if err != nil {
							return err
						}
					}
				}
				// batch listener factory
				for index, listenerFactory := range batchListenersFactory {
					var name = modBatchListenerFactory.Name(index)
					var generatedListeners, errCreate = listenerFactory()
					if errCreate != nil {
						return errCreate
					}
					for _, listener := range generatedListeners {
						err = createWrappedListener(listener, batchConsumerGroupFactory(listener.Handle, monitor, logger, name))
						if err != nil {
							return err
						}
					}
				}
				return wg.Wait()
			}

			return cnt.Call(
				runE,
				di.Constraint(3, di.Optional(true), modListener.Modifier()),
				di.Constraint(4, di.Optional(true), modBatchListener.Modifier()),
				di.Constraint(5, di.Optional(true), modListenerFactory.Modifier()),
				di.Constraint(6, di.Optional(true), modBatchListenerFactory.Modifier()),
			)
		}
		cmd = &cobra.Command{
			Use:   "listener [name...]",
			Short: "Run various Kafka listeners",
			Long: `Run various Kafka listeners.

You can use glob syntax in listener names, for more information see https://github.com/gobwas/glob#syntax`,
			RunE: runE,
		}
	)

	return cmd
}

func consumerGroupFactory(handler client.Consume, monitor monitor.Monitor, logger *zap.Logger, name string) createHandler {
	return func(subs kafkaapi.Subscription) sarama.ConsumerGroupHandler {
		return client.NewConsumerWrapper(
			handler,
			monitor.WithConnection(subs.Connection),
			logger.Named(listenerPrefix+name),
		)
	}
}

func batchConsumerGroupFactory(
	handler client.ConsumeBatch,
	monitor monitor.Monitor,
	logger *zap.Logger, name string) createHandler {
	return func(subs kafkaapi.Subscription) sarama.ConsumerGroupHandler {
		return client.NewBatchConsumerWrapper(
			handler, subs.Size,
			monitor.WithConnection(subs.Connection),
			logger.Named(listenerPrefix+name),
		)
	}
}
